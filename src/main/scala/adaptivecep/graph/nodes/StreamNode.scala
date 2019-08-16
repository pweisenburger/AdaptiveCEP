package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.Privacy._
import adaptivecep.publishers.Publisher._
import akka.actor.{ActorRef, PoisonPill}
import akka.stream. OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class StreamNode(
                       requirements: Set[Requirement],
                       publisherName: String,
                       publishers: Map[String, ActorRef],
                       frequencyMonitorFactory: MonitorFactory,
                       latencyMonitorFactory: MonitorFactory,
                       createdCallback: Option[() => Any],
                       eventCallback: Option[(Event) => Any]
                       , privacyContext: PrivacyContext = NoPrivacyContext
                     )
//                     (implicit val privacyContext: PrivacyContext = NoPrivacyContext)
  extends LeafNode {

  var subscriptionAcknowledged: Boolean = false
  var parentReceived: Boolean = false
  var publisher: ActorRef = self

  override def postCreated(): Unit =
  {
    publisher = publishers(publisherName)
    publisher ! Subscribe
    println("subscribing to publisher", publisher.path)
  }


  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription(ref) if sender() == publisher =>
      subscriptionAcknowledged = true
      //      val initVector = "ABCDEFGHIJKLMNOP"
      //      val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
      //      val secret = "mysecret"
      //      val spec = new PBEKeySpec(secret.toCharArray, "1234".getBytes(), 65536, 128)
      //      val factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
      //      val key = factory.generateSecret(spec).getEncoded
      //      val skeySpec = new SecretKeySpec(key, "AES")
      //      implicit val encryption: Encryption = CryptoAES(skeySpec,iv)

      ref.getSource.to(Sink.foreach(a => {
        emitEvent(a)
        //        privacyContext match {
        //          case SgxPrivacyContext(trustedHosts, remoteObject, conversionRules)
        //            =>
        //            val encEvent = getEncryptedEvent(a,conversionRules(publisherName))
        //            emitEvent(encEvent)
        //          case NoPrivacyContext => emitEvent(a)
        //        }
      })).run(materializer)
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
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
