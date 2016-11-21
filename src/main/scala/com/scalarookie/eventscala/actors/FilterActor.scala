package com.scalarookie.eventscala.actors

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class FilterActor(filter: Filter, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  /* TODO */ println(s"Node `${self.path.name}` created; representing `$filter`.")

  val subquery: Query = filter.subquery
  val operator: Operator = filter.operator
  val operand1: Either[Int, Any] = filter.operand1
  val operand2: Either[Int, Any] = filter.operand2

  val configuration = new Configuration
  lazy val serviceProvider = EPServiceProviderManager.getProvider(s"${self.path.name}", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  configuration.addEventType("subquery", subqueryElementNames, subqueryElementClasses.asInstanceOf[Array[AnyRef]])

  def getEplFrom(operand: Either[Int, Any]): String = operand match {
    case Left(int) => s"sq.e$int"
    case Right(any) => any match {
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

  val eplStatement: EPStatement = administrator.createEPL(
    s"select * from subquery as sq where $operand1Epl $operatorEpl $operand2Epl")

  // TODO
  println(s"select * from subquery as sq where $operand1Epl $operatorEpl $operand2Epl")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      // TODO
      val subqueryElementValues: Array[AnyRef] = subqueryElementNames.map(s => newEvents(0).get(s))
      //val subqueryElementValues: Array[AnyRef] = newEvents(0).get("sq").asInstanceOf[Array[AnyRef]]
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
    }
  })

  val subqueryActor: ActorRef = subquery match {
    case stream: Stream => context.actorOf(Props(
      new StreamActor(stream, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-stream")
    case join: Join => context.actorOf(Props(
      new JoinActor(join, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-join")
    case select: Select => context.actorOf(Props(
      new SelectActor(select, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-select")
    case filter: Filter => context.actorOf(Props(
      new FilterActor(filter, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-filter")
  }

  override def receive: Receive = {
    case event: Event =>
      if (sender == subqueryActor) {
        runtime.sendEvent(Event.getArrayOfValuesFrom(event), "subquery")
      }
  }

}
