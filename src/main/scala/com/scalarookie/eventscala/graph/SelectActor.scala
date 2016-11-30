package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class SelectActor(select: Select, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor with EsperEngine {

  val actorName: String = self.path.name
  override val esperServiceProviderUri: String = actorName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(select.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val elementIdsEpl: String = select.elementIds.map(i => s"sq.e$i").mkString(", ")

  val eplStatement: EPStatement = createEplStatement(s"select $elementIdsEpl from subquery as sq")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = select.elementIds.map(i => s"sq.e$i").map(newEvents(0).get(_)).toArray
      val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(select)
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
    }
  })

  val subqueryActor: ActorRef =
    OperatorActor.createChildActorFrom(select.subquery, actorName, 1, context, publishers, Some(root.getOrElse(self)))

  override def receive: Receive = {
    case event: Event if sender == subqueryActor =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
  }

}
