package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class SelectNode(select: Select, publishers: Map[String, ActorRef]) extends Actor with EsperEngine {

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(select.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val elementIdsEpl: String = select.elementIds.map(i => s"sq.e$i").mkString(", ")

  val eplStatement: EPStatement = createEplStatement(s"select $elementIdsEpl from subquery as sq")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = select.elementIds.map(i => s"sq.e$i").map(newEvents(0).get).toArray
      val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(select)
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      context.parent ! event
    }
  })

  val subqueryNode: ActorRef = Node.createChildNodeFrom(select.subquery, nodeName, 1, publishers, context)

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
  }

}
