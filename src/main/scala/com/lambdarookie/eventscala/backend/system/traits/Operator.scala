package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.data.Queries.Query
import com.lambdarookie.eventscala.graph.factory.OperatorFactory

/**
  * Created by monur.
  */
trait Operator {
  val system: System
  val host: Host
  val query: Query
  val inputs: Seq[Operator]
  val outputs: Set[Operator]

  def createChildOperator(subQuery: Query): Operator =
    OperatorFactory.createOperator(system, subQuery, Set(this))
}
