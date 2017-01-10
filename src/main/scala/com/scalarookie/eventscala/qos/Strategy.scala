package com.scalarookie.eventscala.qos

import akka.actor.{ActorContext, ActorRef}
import com.scalarookie.eventscala.caseclasses._

case class LeafNodeData(name: String, query: Query, context: ActorContext)
case class UnaryNodeData(name: String, query: Query, context: ActorContext, childNode: ActorRef)
case class BinaryNodeData(name: String, query: Query, context: ActorContext, childNode1: ActorRef, childNode2: ActorRef)

trait LeafNodeStrategy {

  def onCreated(nodeData: LeafNodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: LeafNodeData): Unit = ()

}

trait UnaryNodeStrategy {

  def onCreated(nodeData: UnaryNodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = ()

}

trait BinaryNodeStrategy {

  def onCreated(nodeData: BinaryNodeData): Unit = ()
  def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = ()
  def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = ()

}
