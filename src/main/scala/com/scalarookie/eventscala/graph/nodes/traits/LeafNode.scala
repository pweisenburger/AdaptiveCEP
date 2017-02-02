package com.scalarookie.eventscala.graph.nodes.traits

import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.graph.qos._

trait LeafNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  val frequencyMonitor: LeafNodeMonitor = frequencyMonitorFactory.createLeafNodeMonitor
  val latencyMonitor: LeafNodeMonitor = latencyMonitorFactory.createLeafNodeMonitor
  val nodeData: LeafNodeData = LeafNodeData(name, query, context)

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

}
