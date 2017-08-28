package com.lambdarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.system.EventSource
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.nodes.traits._
import com.lambdarookie.eventscala.graph.monitors._
import com.lambdarookie.eventscala.publishers.Publisher._

case class StreamNode(
                       system: System,
                       query: StreamQuery,
                       operator: EventSource,
                       publishers: Map[String, ActorRef],
                       monitors: Set[_ <: Monitor],
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
      monitors.foreach(_.onMessageReceive(unhandledMessage, nodeData))
  }

}
