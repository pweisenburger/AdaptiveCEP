package com.lambdarookie.eventscala.backend.graph.nodes.traits

import akka.actor.{Actor, ActorRef}
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.backend.graph.monitors._
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.data.Events.{Created, Event}
import com.lambdarookie.eventscala.backend.graph.factory.NodeFactory

trait Node extends Actor {

  val name: String = self.path.name

  val system: System
  val query: Query
  val operator: Operator
  val publishers: Map[String, ActorRef]
  val nodeData: NodeData
  val monitors: Set[_ <: Monitor]
  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  system.addNodeOperatorPair(self, operator)
  system.addQuery(query)

  def emitCreated(): Unit = {
    if (createdCallback.isDefined) createdCallback.get.apply() else context.parent ! Created
    monitors.foreach(_.onCreated(nodeData))
  }

  def emitEvent(event: Event): Unit = {
    if (eventCallback.isDefined) eventCallback.get.apply(event) else context.parent ! event
    monitors.foreach(_.onEventEmit(event, nodeData))
  }

  def createChildNode(id: Int, query: Query, childOperator: Operator): ActorRef =
    NodeFactory.createNode(
      system, context, query, childOperator, publishers, monitors.map(_.copy),
      None, None, s"$name-$id-")

}
