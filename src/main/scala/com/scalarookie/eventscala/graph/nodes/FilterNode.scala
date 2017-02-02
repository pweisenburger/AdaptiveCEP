package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.qos._

case class FilterNode(
    query: FilterQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends Node {

  val childNode: ActorRef = createChildNode(1, query.sq)

  val nodeData: UnaryNodeData = UnaryNodeData(name, query, context, childNode)

  val frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  val latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor

  def emitCreated(): Unit = {
    if (createdCallback.isDefined) createdCallback.get.apply() else context.parent ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (eventCallback.isDefined) eventCallback.get.apply(event) else context.parent ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

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
