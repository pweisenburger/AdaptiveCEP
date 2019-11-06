package adaptivecep.graph.nodes

import adaptivecep.privacy.ConversionRules._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.encryption.{CryptoAES, Encryption}
import adaptivecep.publishers.Publisher._
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class StreamNode(
                       requirements: Set[Requirement],
                       publisherName: String,
                       publishers: Map[String, ActorRef],
                       frequencyMonitorFactory: MonitorFactory,
                       latencyMonitorFactory: MonitorFactory,
                       createdCallback: Option[() => Any],
                       eventCallback: Option[(Event) => Any],
                       privacyContext: Option[PrivacyContext] = None
                     )
  extends LeafNode {

  //  var subscriptionAcknowledged: Boolean = false
  //  var parentReceived: Boolean = false
  //  var publisher: ActorRef = self

  override def postCreated(): Unit = {
    val publisher = publishers(publisherName)
    publisher ! Subscribe
    println("subscribing to publisher", publisher.path)
  }


  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription(ref) /*if sender() == publisher*/ =>
      //      subscriptionAcknowledged = true
      /** *
        * TODO: this encryption keys can be exposed during operators deployment
        * we assume secure deployment phase
        * another approach is to point to the encryption key position in the stream node
        * and only load that key here
        * according to our trust model, even if we expose the private key path here
        * the attacker cannot access the trusted Stream/Publisher node so passing the path to the key
        * here is considered safe
        *
        */

      val initVector = "ABCDEFGHIJKLMNOP"
      val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
      val secret = "mysecret"
      val spec = new PBEKeySpec(secret.toCharArray, "1234".getBytes(), 65536, 128)
      val factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
      val key = factory.generateSecret(spec).getEncoded
      val skeySpec = new SecretKeySpec(key, "AES")
      implicit val encryption: Encryption = CryptoAES(skeySpec, iv)

      ref.getSource.to(Sink.foreach(a => {
        //        emitEvent(a)
        if (privacyContext.isEmpty)
        //        if (conversionRule == None)
          emitEvent(a)
        else {

          privacyContext.get match {
            case NoPrivacyContext =>
              emitEvent(a)
            case SgxPrivacyContext(trustedHosts, remoteObject, conversionRules) =>
              val rule = conversionRules(publisherName)
              val encEvent = getEncryptedEvent(a, rule)
              println(s"Emitting encrypted event for event $a and $encEvent\n")
              emitEvent(encEvent)
            case PhePrivacyContext(interpret, cryptoService, sourceMappers) =>
              val mapper = sourceMappers(publisherName)
              val mappedEvent = mapSource(a,mapper,cryptoService)
              emitEvent(mappedEvent )
              println(s"Emitting encrypted event for event $a and $mappedEvent\n")
              //TODO: encrypt using source conversion rules
            case _ => sys.error("unsupported context yet")
          }
        }
      })).run(materializer)
    case Parent(p1) => {
      parentNode = p1
      //      parentReceived = true
      nodeData = LeafNodeData(name, requirements, context, parentNode)
    }
    case CentralizedCreated =>
      if (!created) {
        created = true
        emitCreated()
      }
    case SourceRequest =>
      source = Source.queue[Event](20000, OverflowStrategy.dropNew).preMaterialize()(materializer)
      future = source._2.runWith(StreamRefs.sourceRef())(materializer)
      sourceRef = Await.result(future, Duration.Inf)
      sender() ! SourceResponse(sourceRef)
    case KillMe => sender() ! PoisonPill
    case Kill =>
    case Controller(c) =>
      controller = c
    case CostReport(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(CostReport(c), nodeData)
      latencyMonitor.onMessageReceive(CostReport(c), nodeData)
    case e: Event => emitEvent(e)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
