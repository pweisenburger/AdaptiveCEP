package adaptivecep.graph.nodes

import akka.actor.ActorRef
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.qos._

case class SelectNode(
    query: SelectQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node {

  val childNode: ActorRef = createChildNode(1, query.sq)

  val nodeData: UnaryNodeData = UnaryNodeData(name, query, context, childNode)

  val frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  val latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor
  //val frequencyReqs: Set[FrequencyRequirement] = query.requirements collect { case fr: FrequencyRequirement => fr }
  //val latencyReqs: Set[LatencyRequirement] = query.requirements collect { case lr: LatencyRequirement => lr }

  val elementToBeRemoved: Int = query match {
    case RemoveElement1Of2(_, _) => 1
    case RemoveElement1Of3(_, _) => 1
    case RemoveElement1Of4(_, _) => 1
    case RemoveElement1Of5(_, _) => 1
    case RemoveElement1Of6(_, _) => 1
    case RemoveElement2Of2(_, _) => 2
    case RemoveElement2Of3(_, _) => 2
    case RemoveElement2Of4(_, _) => 2
    case RemoveElement2Of5(_, _) => 2
    case RemoveElement2Of6(_, _) => 2
    case RemoveElement3Of3(_, _) => 3
    case RemoveElement3Of4(_, _) => 3
    case RemoveElement3Of5(_, _) => 3
    case RemoveElement3Of6(_, _) => 3
    case RemoveElement4Of4(_, _) => 4
    case RemoveElement4Of5(_, _) => 4
    case RemoveElement4Of6(_, _) => 4
    case RemoveElement5Of5(_, _) => 5
    case RemoveElement5Of6(_, _) => 5
    case RemoveElement6Of6(_, _) => 6
  }

  def emitGraphCreated(): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

  def handleEvent2(e1: Any, e2: Any): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event1(e2))
    case 2 => emitEvent(Event1(e1))
  }

  def handleEvent3(e1: Any, e2: Any, e3: Any): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event2(e2, e3))
    case 2 => emitEvent(Event2(e1, e3))
    case 3 => emitEvent(Event2(e1, e2))
  }

  def handleEvent4(e1: Any, e2: Any, e3: Any, e4: Any): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event3(e2, e3, e4))
    case 2 => emitEvent(Event3(e1, e3, e4))
    case 3 => emitEvent(Event3(e1, e2, e4))
    case 4 => emitEvent(Event3(e1, e2, e3))
  }

  def handleEvent5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event4(e2, e3, e4, e5))
    case 2 => emitEvent(Event4(e1, e3, e4, e5))
    case 3 => emitEvent(Event4(e1, e2, e4, e5))
    case 4 => emitEvent(Event4(e1, e2, e3, e5))
    case 5 => emitEvent(Event4(e1, e2, e3, e4))
  }

  def handleEvent6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any): Unit = elementToBeRemoved match {
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
    case GraphCreated if sender() == childNode =>
      emitGraphCreated()
    case event: Event if sender() == childNode => event match {
      case Event1(_) => ??? // Simply throw an exception if control flow ever reaches this point -- which should never happen!
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
