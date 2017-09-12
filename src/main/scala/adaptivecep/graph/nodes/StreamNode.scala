package adaptivecep.graph.nodes

import akka.actor.ActorRef
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.publishers.Publisher._
import adaptivecep.graph.qos._

case class StreamNode(
    query: StreamQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node {

  val publisher: ActorRef = publishers(query.publisherName)

  val nodeData: LeafNodeData = LeafNodeData(name, query, context)

  val frequencyMonitor: LeafNodeMonitor = frequencyMonitorFactory.createLeafNodeMonitor
  val latencyMonitor: LeafNodeMonitor = latencyMonitorFactory.createLeafNodeMonitor
  //val frequencyReqs: Set[FrequencyRequirement] = query.requirements collect { case fr: FrequencyRequirement => fr }
  //val latencyReqs: Set[LatencyRequirement] = query.requirements collect { case lr: LatencyRequirement => lr }

  publisher ! Subscribe

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

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription if sender() == publisher =>
      emitGraphCreated()
    case event: Event if sender() == publisher =>
      emitEvent(event)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
