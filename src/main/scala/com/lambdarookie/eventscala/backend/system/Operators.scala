package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, Query, UnaryQuery}

/**
  * Created by monur.
  */

case class EventSource(system: System, query: Query, outputs: Set[Operator]) extends Operator {
  val host: Host = system.selectHostForOperator(this)
  val inputs = Seq.empty[Operator]
}
case class UnaryOperator(system: System, query: UnaryQuery, outputs: Set[Operator]) extends Operator {
  val host: Host = system.selectHostForOperator(this)
  val inputs = Seq(createChildOperator(query.sq))
}
case class BinaryOperator(system: System, query: BinaryQuery, outputs: Set[Operator]) extends Operator {
  val host: Host = system.selectHostForOperator(this)
  val inputs = Seq(createChildOperator(query.sq1), createChildOperator(query.sq2))
}
