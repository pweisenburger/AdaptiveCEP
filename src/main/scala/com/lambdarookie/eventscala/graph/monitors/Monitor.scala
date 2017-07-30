package com.lambdarookie.eventscala.graph.monitors

import akka.actor.{ActorContext, ActorRef}
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.backend.system.traits.System

case class LeafNodeData(name: String, query: Query, system: System, context: ActorContext)
case class UnaryNodeData(name: String, query: Query, system: System, context: ActorContext, childNode: ActorRef)
case class BinaryNodeData(name: String, query: Query, system: System, context: ActorContext, childNode1: ActorRef, childNode2: ActorRef)

trait LeafNodeMonitor {

  def onCreated(nodeData: LeafNodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: LeafNodeData): Unit = ()

}

trait UnaryNodeMonitor {

  def onCreated(nodeData: UnaryNodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = ()

}

trait BinaryNodeMonitor {

  def onCreated(nodeData: BinaryNodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = ()

}

trait MonitorFactory {

  def createLeafNodeMonitor: LeafNodeMonitor
  def createUnaryNodeMonitor: UnaryNodeMonitor
  def createBinaryNodeMonitor: BinaryNodeMonitor

}
