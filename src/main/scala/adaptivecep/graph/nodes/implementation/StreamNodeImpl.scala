package adaptivecep.graph.nodes.implementation

import adaptivecep.data.Events.{DependenciesRequest, DependenciesResponse, Event, PrepareForUpdate, TransferState}
import adaptivecep.data.Queries.StreamQuery
import adaptivecep.graph.nodes.traits.LeafNode
import adaptivecep.graph.qos.{FullState, LeafNodeData, LeafNodeMonitor, MonitorFactory}
import adaptivecep.publishers.Publisher.AcknowledgeSubscription
import akka.actor.ActorRef

case class StreamNodeImpl(
                           query: StreamQuery,
                           publishers: Map[String, ActorRef],
                           frequencyMonitorFactory: MonitorFactory,
                           latencyMonitorFactory: MonitorFactory,
                           createdCallback: Option[() => Any],
                           eventCallback: Option[(Event) => Any])
  extends LeafNode {

  val publisher: ActorRef = publishers(query.publisherName)

  def this(args: Tuple6[StreamQuery, Map[String, ActorRef],
    MonitorFactory, MonitorFactory, Option[()=>Any], Option[(Event)=>Any]]) = {
    this(args._1, args._2, args._3, args._4, args._5, args._6)
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription if sender() == publisher =>
      emitCreated()
    case event: Event if (sender() == publisher) =>
      emitEvent(event)
    case PrepareForUpdate =>
      val fullState = FullState(nodeData, frequencyMonitor, latencyMonitor)
      sender() ! TransferState(fullState)
    case TransferState(state) =>
      nodeData = state.data.asInstanceOf[LeafNodeData]
      frequencyMonitor = state.frequencyMonitor.asInstanceOf[LeafNodeMonitor]
      latencyMonitor = state.latencyMonitor.asInstanceOf[LeafNodeMonitor]

      //uncomment in case of local simulation
      //UpdateGeneratorLocal.setFinish(System.currentTimeMillis())
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }
}
