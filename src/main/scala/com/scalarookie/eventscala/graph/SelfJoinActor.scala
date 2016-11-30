package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class SelfJoinActor(join: Join, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor with EsperEngine {

  require(join.subquery1 == join.subquery2)

  val actorName: String = self.path.name
  override val esperServiceProviderUri: String = actorName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery1)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val window1Epl: String = JoinActor.getEplFrom(join.window1)
  val window2Epl: String = JoinActor.getEplFrom(join.window2)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery.$window1Epl as lhs, subquery.$window2Epl as rhs")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val lhsElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("lhs").asInstanceOf[Array[AnyRef]]
        val rhsElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("rhs").asInstanceOf[Array[AnyRef]]
        val lhsAndRhsElementValues: Array[AnyRef] = lhsElementValues ++ rhsElementValues
        val lhsAndRhsElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
        val event: Event = Event.getEventFrom(lhsAndRhsElementValues, lhsAndRhsElementClasses)
        if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
      }
    }
  })

  val subqueryActor: ActorRef =
    OperatorActor.createChildActorFrom(join.subquery1, actorName, 1, context, publishers, Some(root.getOrElse(self)))

  override def receive: Receive = {
    case event: Event if sender == subqueryActor =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
  }

}
