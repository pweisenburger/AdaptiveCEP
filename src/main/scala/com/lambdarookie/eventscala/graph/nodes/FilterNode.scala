package com.lambdarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.system.UnaryOperator
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.nodes.traits._
import com.lambdarookie.eventscala.graph.qos._

case class FilterNode(
    system: System,
    query: FilterQuery,
    operator: UnaryOperator,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any],
    testId: String)
  extends UnaryNode {

  system.nodesToOperatorsVar() = system.nodesToOperators.now + (self -> operator)

  override def receive: Receive = {
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode =>
      if (query.cond(event)) emitEvent(event)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
