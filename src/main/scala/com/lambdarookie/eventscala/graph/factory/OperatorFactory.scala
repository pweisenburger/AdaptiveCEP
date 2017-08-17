package com.lambdarookie.eventscala.graph.factory

import com.lambdarookie.eventscala.backend.system.{BinaryOperator, EventSource, UnaryOperator}
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, LeafQuery, Query, UnaryQuery}

/**
  * Created by monur.
  */
object OperatorFactory {
  def createOperator(system: System, query: Query, outputs: Set[Operator]): Operator = {
    val op = query match {
      case q: LeafQuery => EventSource(system, q, outputs)
      case q: UnaryQuery => UnaryOperator(system, q, outputs)
      case q: BinaryQuery => BinaryOperator(system, q, outputs)
    }
    system.addOperator(op)
    op
  }
}
