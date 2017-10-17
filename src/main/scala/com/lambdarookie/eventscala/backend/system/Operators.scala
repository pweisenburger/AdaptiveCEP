package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, LeafQuery, UnaryQuery}

/**
  * Created by monur.
  */

trait EventSource extends Operator {
  val query: LeafQuery

  val inputs: Seq[Operator] = Seq.empty
}
trait UnaryOperator extends Operator {
  val query: UnaryQuery

  val inputs = Seq(createChildOperator(id + "-" + Operator.SINGLE_CHILD, query.sq))
}
trait BinaryOperator extends Operator {
  val query: BinaryQuery

  val inputs = Seq( createChildOperator(id + "-" + Operator.LEFT_CHILD, query.sq1),
    createChildOperator(id + "-" + Operator.RIGHT_CHILD, query.sq2))
}


private[backend] sealed trait OperatorImpl extends Operator {
  var host: Host = cepSystem.chooseHost(this)

  private[backend] def move(to: Host): Unit = host = to
}

private[backend] case class EventSourceImpl(id: String, cepSystem: CEPSystem, query: LeafQuery, outputs: Set[Operator])
  extends EventSource with OperatorImpl
private[backend] case class UnaryOperatorImpl(id: String, cepSystem: CEPSystem, query: UnaryQuery, outputs: Set[Operator])
  extends UnaryOperator with OperatorImpl
private[backend] case class BinaryOperatorImpl(id: String, cepSystem: CEPSystem, query: BinaryQuery, outputs: Set[Operator])
  extends BinaryOperator with OperatorImpl
