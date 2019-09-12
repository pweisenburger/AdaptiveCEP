package adaptivecep.graph.nodes.implementation

import adaptivecep.data.Events.{Created, DependenciesRequest, DependenciesResponse, Event, PrepareForUpdate, TransferState}
import adaptivecep.data.Queries.FilterQuery
import adaptivecep.graph.nodes.traits.UnaryNode
import adaptivecep.graph.qos.{FullState, MonitorFactory, UnaryNodeData, UnaryNodeMonitor}
import akka.actor.ActorRef

case class FilterNodeImpl(
    query: FilterQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any],
    childNode: ActorRef)
  extends UnaryNode {

  def this(args: Tuple7[FilterQuery, Map[String, ActorRef],
    MonitorFactory, MonitorFactory, Option[()=>Any], Option[(Event)=>Any], ActorRef]) = {
    this(args._1, args._2, args._3, args._4, args._5, args._6, args._7)
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode =>
      if (query.cond(event)) emitEvent(event)
    case PrepareForUpdate =>
      val fullState = FullState(nodeData, frequencyMonitor, latencyMonitor)
      sender() ! TransferState(fullState)
    case TransferState(state) =>
      nodeData = state.data.asInstanceOf[UnaryNodeData]
      frequencyMonitor = state.frequencyMonitor.asInstanceOf[UnaryNodeMonitor]
      latencyMonitor = state.latencyMonitor.asInstanceOf[UnaryNodeMonitor]

    //uncomment in case of local simulation
    //UpdateGeneratorLocal.setFinish(System.currentTimeMillis())
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }
}
