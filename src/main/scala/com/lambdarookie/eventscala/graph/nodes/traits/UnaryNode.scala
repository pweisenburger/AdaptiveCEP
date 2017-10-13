package com.lambdarookie.eventscala.graph.nodes.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.system.UnaryOperator
import com.lambdarookie.eventscala.backend.system.traits.Operator
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.monitors._

trait UnaryNode extends Node {
  override val query: UnaryQuery
  override val operator: UnaryOperator

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  private val childOperator: Operator = operator.inputs.head
  val childNode: ActorRef = createChildNode(1, query.sq, childOperator)

  val nodeData: UnaryNodeData = UnaryNodeData(name, query, system, context, childNode)
}
