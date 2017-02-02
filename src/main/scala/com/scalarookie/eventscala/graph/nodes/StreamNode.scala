package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.nodes.traits._
import com.scalarookie.eventscala.publishers.Publisher._
import com.scalarookie.eventscala.graph.qos._

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
    case AcknowledgeSubscription if sender() == publisher =>
      emitCreated()
    case event: Event if sender() == publisher =>
      emitEvent(event)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
