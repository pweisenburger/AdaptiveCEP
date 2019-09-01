package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.sgx.{EventProcessorClient, EventProcessorServer}
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  *
  * @param requirements
  * @param cond
  * @param publishers
  * @param frequencyMonitorFactory
  * @param latencyMonitorFactory
  * @param createdCallback
  * @param eventCallback
  * @param privacyContext represents the privacy context of the current query
  *                       whether we would like to encrypt or not
  */
case class FilterNode(
                       requirements: Set[Requirement],
                       cond: Event => Boolean,
                       publishers: Map[String, ActorRef],
                       frequencyMonitorFactory: MonitorFactory,
                       latencyMonitorFactory: MonitorFactory,
                       createdCallback: Option[() => Any],
                       eventCallback: Option[(Event) => Any]
                       ,privacyContext: PrivacyContext = NoPrivacyContext
                     )
  extends UnaryNode {

  var parentReceived: Boolean = false
  var childCreated: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      childCreated = true
    case CentralizedCreated =>
      if (!created) {
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case SourceRequest =>
      source = Source.queue[Event](20000, OverflowStrategy.dropNew).preMaterialize()(materializer)
      future = source._2.runWith(StreamRefs.sourceRef())(materializer)
      sourceRef = Await.result(future, Duration.Inf)
      sender() ! SourceResponse(sourceRef)
    case SourceResponse(ref) =>
      val s = sender()
      ref.getSource.to(Sink foreach (e => {
        processEvent(e, s)
      })).run(materializer)
    case Child1(c) => {
      childNode = c
      c ! SourceRequest
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
      emitCreated()
    }
    case ChildUpdate(_, a) => {
      emitCreated()
      childNode = a
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      scheduledTask.cancel()
      lmonitor.get.scheduledTask.cancel()
    case Controller(c) =>
      controller = c
    case CostReport(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(CostReport(c), nodeData)
      latencyMonitor.onMessageReceive(CostReport(c), nodeData)
    case e: Event => processEvent(e, sender())
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }


  private var remoteObject: Option[EventProcessorServer] = None

  def getRemoteObject(address: String, port: Int): EventProcessorServer = {
    if(remoteObject.isEmpty)
      {
        val eventProcessorClient = EventProcessorClient("13.80.151.52", 60000)
        val server = eventProcessorClient.lookupObject()
        remoteObject = Some(server)
        remoteObject.get
      }
    else
      remoteObject.get
  }

  def processEvent(event: Event, sender: ActorRef): Unit = {
    if (sender == childNode) {
      privacyContext match {
        case NoPrivacyContext =>
          if (cond(event))
            emitEvent(event)

        case SgxPrivacyContext(address,port, conversionRules) =>
          try {
            if (getRemoteObject(address,port).applyPredicate(cond, event)) {
              emitEvent(event)
            }
          } catch {
            case e: Exception => println("\n[SGX Service unable to apply predicate]\n")
          }

        case _
        => println("unexpected context!")

      }
    }
  }
}
