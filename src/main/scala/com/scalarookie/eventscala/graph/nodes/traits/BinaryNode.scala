package com.scalarookie.eventscala.graph.nodes.traits

import akka.actor.ActorRef
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.qos._

trait BinaryNode extends Node {

  override val query: BinaryQuery

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  val childNode1: ActorRef = createChildNode(1, query.sq1)
  val childNode2: ActorRef = createChildNode(2, query.sq2)

  val frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  val latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  val nodeData: BinaryNodeData = BinaryNodeData(name, query, context, childNode1, childNode2)

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