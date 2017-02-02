package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.publishers.Publisher._
import com.scalarookie.eventscala.graph.qos._

case class StreamNode(
    query: StreamQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends Node {

  val publisher: ActorRef = publishers(query.publisherName)

  val nodeData: LeafNodeData = LeafNodeData(name, query, context)

  val frequencyMonitor: LeafNodeMonitor = frequencyMonitorFactory.createLeafNodeMonitor
  val latencyMonitor: LeafNodeMonitor = latencyMonitorFactory.createLeafNodeMonitor

  publisher ! Subscribe

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
    case AcknowledgeSubscription if sender() == publisher =>
      emitCreated()
    case event: Event if sender() == publisher =>
      emitEvent(event)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
