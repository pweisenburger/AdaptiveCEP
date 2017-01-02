package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

object FilterNode {

  def getEplFrom(operand: Either[Int, Any]): String = operand match {
    case Left(id) => s"sq.e$id"
    case Right(literal) => literal match {
      // Have a look at "Table 5.1. Types of EPL constants". `java.lang.Byte` has been omitted for simplicity's sake.
      // http://www.espertech.com/esper/release-5.5.0/esper-reference/html/epl_clauses.html#epl-syntax-datatype
      case s: String => s"'$s'" // TODO ???
      case i: Integer => i.toString
      case l: java.lang.Long => s"${l}l"
      case f: java.lang.Float => s"${f}f"
      case d: java.lang.Double => d.toString
      case b: java.lang.Boolean => b.toString
    }
  }

  def getEplFrom(operator: Operator): String = operator match {
    case Equal => "="
    case NotEqual => "!="
    case Greater => ">"
    case GreaterEqual => ">="
    case Smaller => "<"
    case SmallerEqual => "<="
  }

}

class FilterNode(filter: Filter,
                 publishers: Map[String, ActorRef],
                 frequencyStrategy: FrequencyStrategy,
                 latencyStrategy: PathLatencyUnaryNodeStrategy)
  extends UnaryNode(filter,
                    frequencyStrategy,
                    latencyStrategy,
                    publishers) {

  val operand1Epl: String = FilterNode.getEplFrom(filter.operand1)
  val operand2Epl: String = FilterNode.getEplFrom(filter.operand2)
  val operatorEpl: String = FilterNode.getEplFrom(filter.operator)

  val eplString: String = s"select * from subquery as sq where $operand1Epl $operatorEpl $operand2Epl"

  def eventBean2Event(eventBean: EventBean): Event = {
    val subqueryElementValues: Array[AnyRef] = subqueryElementNames.map(eventBean.get)
    Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
  }

  createEplStatementAndAddListener(eplString, eventBean2Event)

}
