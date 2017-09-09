package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.data.Queries.Query

/**
  * Created by monur.
  */
trait Operator {
  val id: String
  val cepSystem: CEPSystem
  val host: Host
  val query: Query
  val inputs: Seq[Operator]
  val outputs: Set[Operator]

  protected def createChildOperator(id: String, subQuery: Query): Operator =
    cepSystem.createOperator(id, subQuery, Set(this))

  override def toString: String = s"Operator$id"
}

object Operator {
  val ROOT: String = "00"
  val SINGLE_CHILD: String = "0"
  val LEFT_CHILD: String = "1"
  val RIGHT_CHILD: String = "2"
}
