package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.data.Queries.Query

/**
  * Created by monur.
  */
trait Operator {
  val id: String
  val cepSystem: CEPSystem
  val query: Query
  val inputs: Seq[Operator]
  val outputs: Set[Operator]

  def host: Host


  private[backend] def createChildOperator(id: String, subQuery: Query): Operator =
    cepSystem.createOperator(id, subQuery, Set(this))

  def getDescendants: Set[Operator] = inputs.toSet ++ inputs.flatMap(_.getDescendants)

  override def toString: String = s"Operator$id"
}

object Operator {
  val ROOT: String = "root"
  val SINGLE_CHILD: String = "single"
  val LEFT_CHILD: String = "left"
  val RIGHT_CHILD: String = "right"
}
