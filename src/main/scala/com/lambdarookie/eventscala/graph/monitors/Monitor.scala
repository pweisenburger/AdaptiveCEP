package com.lambdarookie.eventscala.graph.monitors


import akka.actor.{ActorContext, ActorRef}
import com.lambdarookie.eventscala.backend.system.traits.System
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._

trait NodeData { val name: String; val query: Query; val system: System; val context: ActorContext }

case class LeafNodeData(name: String, query: Query, system: System,
                        context: ActorContext) extends  NodeData
case class UnaryNodeData(name: String, query: Query, system: System,
                         context: ActorContext, childNode: ActorRef) extends NodeData
case class BinaryNodeData(name: String, query: Query, system: System,
                          context: ActorContext, childNode1: ActorRef, childNode2: ActorRef) extends NodeData

trait Monitor {
  def onCreated(nodeData: NodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: NodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: NodeData): Unit = ()

  def copy: Monitor
}
