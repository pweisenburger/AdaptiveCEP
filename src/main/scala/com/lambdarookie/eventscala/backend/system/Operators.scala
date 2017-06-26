package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, Query, UnaryQuery}

/**
  * Created by monur.
  */
//case class ConjunctionOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class DisjunctionOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class DropElemOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class FilterOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class JoinOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class SelfJoinOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class SequenceJoinOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator
//case class StreamOperator(host: Host, inputs: Set[Operator], outputs: Set[Operator]) extends Operator

case class EventSource(testId: String, system: System, host: Host, query: Query, outputs: Set[Operator]) extends Operator {
  val inputs = Seq.empty[Operator]
}
case class UnaryOperator(testId: String, system: System, host: Host, query: UnaryQuery, outputs: Set[Operator]) extends Operator {
  val inputs = Seq(createChildOperator(s"$testId-1", query.sq))
}
case class BinaryOperator(testId: String, system: System, host: Host, query: BinaryQuery, outputs: Set[Operator]) extends Operator {
  val inputs = Seq(createChildOperator(s"$testId-1", query.sq1), createChildOperator(s"$testId-2", query.sq2))
}
