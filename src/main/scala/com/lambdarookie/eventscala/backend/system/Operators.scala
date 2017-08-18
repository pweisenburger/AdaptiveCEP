package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, Query, UnaryQuery}

/**
  * Created by monur.
  */

case class EventSource(id: String, system: System, query: Query, outputs: Set[Operator]) extends Operator {
  val host: Host = system.placeOperator(this)
  val inputs = Seq.empty[Operator]
}
case class UnaryOperator(id: String, system: System, query: UnaryQuery, outputs: Set[Operator]) extends Operator {
  val host: Host = system.placeOperator(this)
  val inputs = Seq(createChildOperator(id + "-" + Operator.SINGLE_CHILD, query.sq))
}
case class BinaryOperator(id: String, system: System, query: BinaryQuery, outputs: Set[Operator]) extends Operator {
  val host: Host = system.placeOperator(this)
  val inputs = Seq( createChildOperator(id + "-" + Operator.LEFT_CHILD, query.sq1),
                    createChildOperator(id + "-" + Operator.RIGHT_CHILD, query.sq2))
}
