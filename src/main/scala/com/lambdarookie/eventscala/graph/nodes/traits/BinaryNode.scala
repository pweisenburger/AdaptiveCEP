package com.lambdarookie.eventscala.graph.nodes.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.system.BinaryOperator
import com.lambdarookie.eventscala.backend.system.traits.Operator
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.monitors._

trait BinaryNode extends Node {
  override val query: BinaryQuery
  override val operator: BinaryOperator

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  private val childOperator1: Operator = operator.inputs.head
  private val childOperator2: Operator = operator.inputs(1)

  val childNode1: ActorRef = createChildNode(1, query.sq1, childOperator1)
  val childNode2: ActorRef = createChildNode(2, query.sq2, childOperator2)

  val nodeData: BinaryNodeData = BinaryNodeData(name, query, system, context, childNode1, childNode2)
}