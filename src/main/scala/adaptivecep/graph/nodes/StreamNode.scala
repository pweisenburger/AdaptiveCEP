package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.encryption.{CryptoAES, Encryption}
import adaptivecep.publishers.Publisher._
import akka.NotUsed
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.{KillSwitches, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source, StreamRefs}
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
    eventCallback: Option[(Event) => Any])
                     (implicit val privacyContext: PrivacyContext = NoPrivacyContext)
  extends LeafNode {

  val publisher: ActorRef = publishers(publisherName)
  var subscriptionAcknowledged: Boolean = false
  var parentReceived: Boolean = false

  publisher ! Subscribe
  println("subscribing to publisher", publisher.path)


  def applyTransformer(data: Any, transformer: Transformer) (implicit encryption: Encryption): Any ={
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => encrypt(data,encryption)
    }
  }

  def getEncryptedEvent(e: Event, conversionRule: EventConversionRule)(implicit encryption: Encryption): EncEvent = {
    (e,conversionRule) match {
      case (Event1(e1),er: Event1Rule) => EncEvent1(applyTransformer(e1,er.tr1),er)
      case (Event2(e1,e2), er2: Event2Rule) =>
        EncEvent2(
          applyTransformer(e1,er2.tr1),
          applyTransformer(e2,er2.tr2),
          er2)
      case (Event3(e1,e2,e3), er3: Event3Rule) =>
        EncEvent3(
          applyTransformer(e1,er3.tr1),
          applyTransformer(e2,er3.tr2),
          applyTransformer(e3,er3.tr3),
          er3
        )
      case (Event4(e1,e2,e3,e4), er4: Event4Rule) =>
        EncEvent4(
          applyTransformer(e1,er4.tr1),
          applyTransformer(e2,er4.tr2),
          applyTransformer(e3,er4.tr3),
          applyTransformer(e4,er4.tr4),
          er4
        )
      case (Event5(e1,e2,e3,e4,e5), er5: Event5Rule) =>
        EncEvent5(
          applyTransformer(e1,er5.tr1),
          applyTransformer(e2,er5.tr2),
          applyTransformer(e3,er5.tr3),
          applyTransformer(e4,er5.tr4),
          applyTransformer(e5,er5.tr5),
          er5
        )
      case (Event6(e1,e2,e3,e4,e5,e6), er6: Event6Rule) =>
        EncEvent6(
          applyTransformer(e1,er6.tr1),
          applyTransformer(e2,er6.tr2),
          applyTransformer(e3,er6.tr3),
          applyTransformer(e4,er6.tr4),
          applyTransformer(e5,er6.tr5),
          applyTransformer(e6,er6.tr6),
          er6
        )
    }
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription(ref) if sender() == publisher =>
      subscriptionAcknowledged = true
      val initVector = "ABCDEFGHIJKLMNOP"
      val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
      val secret = "mysecret"
      val spec = new PBEKeySpec(secret.toCharArray, "1234".getBytes(), 65536, 128)
      val factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
      val key = factory.generateSecret(spec).getEncoded
      val skeySpec = new SecretKeySpec(key, "AES")
      implicit val encryption: Encryption = CryptoAES(skeySpec,iv)

      ref.getSource.to(Sink.foreach(a =>{
        emitEvent(a)
        privacyContext match {
          case adaptivecep.privacy.Privacy.SgxPrivacyContext(trustedHosts, remoteObject, conversionRules)
            =>
            val encEvent = getEncryptedEvent(a,conversionRules(publisherName))
            emitEvent(encEvent)
          case NoPrivacyContext => emitEvent(a)
        }
      })).run(materializer)
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = LeafNodeData(name, requirements, context, parentNode)
    }
    case CentralizedCreated =>
      if(!created){
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
