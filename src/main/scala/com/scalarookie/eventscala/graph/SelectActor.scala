package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class SelectActor(select: Select, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor with EsperEngine {

  val actorName: String = self.path.name
  override val esperServiceProviderUri: String = actorName

  val subquery: Query = select.subquery
  val elementIds: List[Int] = select.elementIds

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val elementIdsEpl: String = elementIds.map(i => s"sq.e$i").mkString(", ")

  val eplStatement: EPStatement = createEplStatement(s"select $elementIdsEpl from subquery as sq")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = elementIds.map(i => s"sq.e$i").map(s => newEvents(0).get(s)).toArray
      val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(select)
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
    }
  })

  val subqueryActor: ActorRef = subquery match {
    case stream: Stream => context.actorOf(Props(
      new StreamActor(stream, publishers, Some(root.getOrElse(self)))),
      s"$actorName-stream")
    case join: Join => context.actorOf(Props(
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
