package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.data.Queries.Query
import com.lambdarookie.eventscala.graph.factory.OperatorFactory

/**
  * Created by monur.
  */
trait Operator {
  val testId: String
  val system: System
  val host: Host
  val query: Query
  val inputs: Seq[Operator]
  val outputs: Set[Operator]

  def createChildOperator(testId: String, subQuery: Query): Operator =
    OperatorFactory.createOperator(testId, system, subQuery, Set(this))
}
