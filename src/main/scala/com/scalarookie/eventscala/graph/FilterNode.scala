package com.scalarookie.eventscala.graph

import java.time.Clock
import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class FilterNode(filter: Filter, publishers: Map[String, ActorRef]) extends Actor with EsperEngine {

  // TODO Experimental!
  val clock: Clock = Clock.systemDefaultZone

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(filter.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  def getEplFrom(operand: Either[Int, Any]): String = operand match {
    case Left(id) => s"sq.e$id"
    case Right(literal) => literal match {
      // Have a look at "Table 5.1. Types of EPL constants". `java.lang.Byte` has been omitted for simplicity's sake.
      // http://www.espertech.com/esper/release-5.5.0/esper-reference/html/epl_clauses.html#epl-syntax-datatype
      case s: String => s"'$s'"
      case i: Integer => i.toString
      case l: java.lang.Long => s"${l}l"
      case f: java.lang.Float => s"${f}f"
      case d: java.lang.Double => d.toString
      case b: java.lang.Boolean => b.toString
    }
  }

  val operand1Epl: String = getEplFrom(filter.operand1)
  val operand2Epl: String = getEplFrom(filter.operand2)

  def getEplFrom(operator: Operator): String = operator match {
    case Equal => "="
    case NotEqual => "!="
    case Greater => ">"
    case GreaterEqual => ">="
    case Smaller => "<"
    case SmallerEqual => "<="
  }

  val operatorEpl: String = getEplFrom(filter.operator)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery as sq where $operand1Epl $operatorEpl $operand2Epl")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = subqueryElementNames.map(newEvents(0).get)
      val event: Event = Event.getEventFrom(clock.instant, subqueryElementValues, subqueryElementClasses)
      context.parent ! event
    }
  })

  val subqueryNode: ActorRef = Node.createChildNodeFrom(filter.subquery, nodeName, 1, publishers, context)

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
  }

}
