package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.data.Queries.Query
import com.lambdarookie.eventscala.graph.factory.OperatorFactory

/**
  * Created by monur.
  */
trait Operator {
  val id: String
  val system: System
  val host: Host
  val query: Query
  val inputs: Seq[Operator]
  val outputs: Set[Operator]

  def createChildOperator(id: String, subQuery: Query): Operator =
    OperatorFactory.createOperator(id, system, subQuery, Set(this))

  override def toString: String = s"Operator$id"
}

object Operator {
  val ROOT: String = "00"
  val SINGLE_CHILD: String = "0"
  val LEFT_CHILD: String = "1"
  val RIGHT_CHILD: String = "2"
}
