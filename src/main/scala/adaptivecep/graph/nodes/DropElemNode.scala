package adaptivecep.graph.nodes

import akka.actor.{ActorRef, Address, Deploy, PoisonPill, Props}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.remote.RemoteScope
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

case class DropElemNode(
    requirements: Set[Requirement],
    elemToBeDropped: Int,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends UnaryNode {

  var parentReceived: Boolean = false
  var childCreated: Boolean = false

  def handleEvent2(e1: Any, e2: Any): Unit = elemToBeDropped match {
    case 1 => emitEvent(Event1(e2))
    case 2 => emitEvent(Event1(e1))
  }

  def handleEvent3(e1: Any, e2: Any, e3: Any): Unit = elemToBeDropped match {
    case 1 => emitEvent(Event2(e2, e3))
    case 2 => emitEvent(Event2(e1, e3))
    case 3 => emitEvent(Event2(e1, e2))
  }

  def handleEvent4(e1: Any, e2: Any, e3: Any, e4: Any): Unit = elemToBeDropped match {
    case 1 => emitEvent(Event3(e2, e3, e4))
    case 2 => emitEvent(Event3(e1, e3, e4))
    case 3 => emitEvent(Event3(e1, e2, e4))
    case 4 => emitEvent(Event3(e1, e2, e3))
  }

  def handleEvent5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any): Unit = elemToBeDropped match {
    case 1 => emitEvent(Event4(e2, e3, e4, e5))
    case 2 => emitEvent(Event4(e1, e3, e4, e5))
    case 3 => emitEvent(Event4(e1, e2, e4, e5))
    case 4 => emitEvent(Event4(e1, e2, e3, e5))
    case 5 => emitEvent(Event4(e1, e2, e3, e4))
  }

  def handleEvent6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any): Unit = elemToBeDropped match {
    case 1 => emitEvent(Event5(e2, e3, e4, e5, e6))
    case 2 => emitEvent(Event5(e1, e3, e4, e5, e6))
    case 3 => emitEvent(Event5(e1, e2, e4, e5, e6))
    case 4 => emitEvent(Event5(e1, e2, e3, e5, e6))
    case 5 => emitEvent(Event5(e1, e2, e3, e4, e6))
    case 6 => emitEvent(Event5(e1, e2, e3, e4, e5))
  }


  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      childCreated = true
    case CentralizedCreated =>
      if(!created){
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
      ref.getSource.to(Sink foreach(e =>{
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

  def processEvent(event: Event, sender: ActorRef): Unit = {
    processedEvents += 1
    if (sender == childNode) {
      event match {
        case Event1(_) => sys.error("Panic! Control flow should never reach this point!")
        case Event2(e1, e2) => handleEvent2(e1, e2)
        case Event3(e1, e2, e3) => handleEvent3(e1, e2, e3)
        case Event4(e1, e2, e3, e4) => handleEvent4(e1, e2, e3, e4)
        case Event5(e1, e2, e3, e4, e5) => handleEvent5(e1, e2, e3, e4, e5)
        case Event6(e1, e2, e3, e4, e5, e6) => handleEvent6(e1, e2, e3, e4, e5, e6)
      }
    }
  }
}
