package adaptivecep.graph.qos

import akka.actor.{ActorContext, ActorRef}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._

case class LeafNodeData(name: String, requirements: Set[Requirement], context: ActorContext, parent: ActorRef)
case class UnaryNodeData(name: String, requirements: Set[Requirement], context: ActorContext, childNode: ActorRef, parent: ActorRef)
case class BinaryNodeData(name: String, requirements: Set[Requirement], context: ActorContext, childNode1: ActorRef, childNode2: ActorRef, parent: ActorRef)

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
