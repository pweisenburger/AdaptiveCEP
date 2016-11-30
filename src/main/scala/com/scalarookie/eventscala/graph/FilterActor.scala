package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class FilterActor(filter: Filter, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor with EsperEngine {

  val actorName: String = self.path.name
  override val esperServiceProviderUri: String = actorName

  val subquery: Query = filter.subquery
  val operator: Operator = filter.operator
  val operand1: Either[Int, Any] = filter.operand1
  val operand2: Either[Int, Any] = filter.operand2

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(subquery)
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

  val operand1Epl: String = getEplFrom(operand1)
  val operand2Epl: String = getEplFrom(operand2)

  def getEplFrom(operator: Operator): String = operator match {
    case Equal => "="
    case NotEqual => "!="
    case Greater => ">"
    case GreaterEqual => ">="
    case Smaller => "<"
    case SmallerEqual => "<="
  }

  val operatorEpl: String = getEplFrom(operator)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery as sq where $operand1Epl $operatorEpl $operand2Epl")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = subqueryElementNames.map(s => newEvents(0).get(s))
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
    }
  })

  val subqueryActor: ActorRef = subquery match {
    case stream: Stream => context.actorOf(Props(
      new StreamActor(stream, publishers, Some(root.getOrElse(self)))),
      s"$actorName-stream")
    case join: Join if join.subquery1 == join.subquery2 => context.actorOf(Props(
      new SelfJoinActor(join, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join")
    case join: Join if join.subquery1 != join.subquery2 => context.actorOf(Props(
      new JoinActor(join, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join")
    case select: Select => context.actorOf(Props(
      new SelectActor(select, publishers, Some(root.getOrElse(self)))),
      s"$actorName-select")
    case filter: Filter => context.actorOf(Props(
      new FilterActor(filter, publishers, Some(root.getOrElse(self)))),
      s"$actorName-filter")
  }

  override def receive: Receive = {
    case event: Event =>
      if (sender == subqueryActor) {
        sendEvent("subquery", Event.getArrayOfValuesFrom(event))
      }
  }

}
