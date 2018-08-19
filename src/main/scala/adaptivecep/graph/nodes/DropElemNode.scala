package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.ActorRef

case class DropElemNode(
    query: DropElemQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends UnaryNode {

  val elemToBeDropped: Int = query match {
    case de@DropElem(_, _, _) => de.pos
  }

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
      emitCreated()
    case event: Event if sender() == childNode => event match {
      case Event1(_) => sys.error("Panic! Control flow should never reach this point!")
      case Event2(e1, e2) => handleEvent2(e1, e2)
      case Event3(e1, e2, e3) => handleEvent3(e1, e2, e3)
      case Event4(e1, e2, e3, e4) => handleEvent4(e1, e2, e3, e4)
      case Event5(e1, e2, e3, e4, e5) => handleEvent5(e1, e2, e3, e4, e5)
      case Event6(e1, e2, e3, e4, e5, e6) => handleEvent6(e1, e2, e3, e4, e5, e6)
    }
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }
}
