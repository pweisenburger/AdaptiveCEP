package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.publishers.Publisher._
import akka.actor.ActorRef

case class StreamNode(
    query: StreamQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends LeafNode {

  val publisher: ActorRef = publishers(query.publisherName)

  publisher ! Subscribe

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription if sender() == publisher =>
      emitCreated()
    case event: Event if sender() == publisher =>
      emitEvent(event)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
