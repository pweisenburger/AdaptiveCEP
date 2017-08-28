package com.lambdarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.system.UnaryOperator
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.nodes.traits._
import com.lambdarookie.eventscala.graph.monitors._

case class FilterNode(
                       system: System,
                       query: FilterQuery,
                       operator: UnaryOperator,
                       publishers: Map[String, ActorRef],
                       monitors: Set[_ <: Monitor],
                       createdCallback: Option[() => Any],
                       eventCallback: Option[(Event) => Any])
  extends UnaryNode {

  override def receive: Receive = {
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode =>
      if (query.cond(event)) emitEvent(event)
    case unhandledMessage =>
      monitors.foreach(_.onMessageReceive(unhandledMessage, nodeData))
  }

}
