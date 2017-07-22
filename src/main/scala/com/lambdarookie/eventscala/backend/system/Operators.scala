package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, Query, UnaryQuery}

/**
  * Created by monur.
  */

case class EventSource(testId: String, system: System, query: Query, outputs: Set[Operator]) extends Operator {
  val host: Host = system.selectHostForOperator(this)
  val inputs = Seq.empty[Operator]
}
case class UnaryOperator(testId: String, system: System, query: UnaryQuery, outputs: Set[Operator]) extends Operator {
  val host: Host = system.selectHostForOperator(this)
  val inputs = Seq(createChildOperator(s"$testId-1", query.sq))
}
case class BinaryOperator(testId: String, system: System, query: BinaryQuery, outputs: Set[Operator]) extends Operator {
  val host: Host = system.selectHostForOperator(this)
  val inputs = Seq(createChildOperator(s"$testId-1", query.sq1), createChildOperator(s"$testId-2", query.sq2))
}
