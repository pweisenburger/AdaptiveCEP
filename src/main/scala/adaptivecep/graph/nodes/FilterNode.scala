package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.sgx.EventProcessorServer
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
                       eventCallback: Option[(Event) => Any],
                       privacyContext: Option[PrivacyContext] = None
                     )
  extends UnaryNode {

  var parentReceived: Boolean = false
  var childCreated: Boolean = false
  var eventProcessor: Option[EventProcessorServer] = None
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


  override def preStart(): Unit = {
    if (privacyContext.nonEmpty) {
      privacyContext.get match {
        case SgxPrivacyContext(_, client, _) =>
          try {
            eventProcessor = Some(client.lookupObject())
            println("remote object created")
          } catch {
            case e: Exception => println("\n[unable to lookup the remote event processor]\n")
              println(e.getMessage)
          }
        case _ => println("not an SGX context")
      } /// pattern matching
    } ///check if pc is empty
  }

  def processEvent(event: Event, sender: ActorRef): Unit = {
    if (sender == childNode) {
      if (privacyContext.nonEmpty) {
        privacyContext.get match {
          case NoPrivacyContext =>
            if (cond(event))
              emitEvent(event)
          case SgxPrivacyContext(_, _, _) =>
            try {
              if(eventProcessor.nonEmpty) {
                if(eventProcessor.get.applyPredicate(cond,event)) {
                  emitEvent(event)
                } else {
                  println("event dropped!")
                }
              } else {
                println("event processor not initialized")
              }
            } catch {
              case e: Exception => println("\n[SGX Service unable to apply predicate]\n")
                println(e.getMessage)
            }
          case PhePrivacyContext(cryptoService, sourceMappers) =>
            if (cond(event))
              emitEvent(event)
          case PrivacyContextCentralized(interpret, cryptoService, trustedHosts, sourcesSensitivity)
          => println("unexpected context!")
        } /// pattern matching
      } ///check if pc is empty
      else{
        if (cond(event))
          emitEvent(event)
      }

    }
  }
}
