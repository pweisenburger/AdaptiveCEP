package com.scalarookie.eventscala.graph

import java.time.Clock
import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class SelfJoinNode(join: Join, publishers: Map[String, ActorRef]) extends Actor with EsperEngine {

  require(join.subquery1 == join.subquery2)

  // TODO Experimental!
  val clock: Clock = Clock.systemDefaultZone

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery1)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(instances) => s"win:length($instances)"
    case LengthTumbling(instances) => s"win:length_batch($instances)"
    case TimeSliding(seconds) => s"win:time($seconds)"
    case TimeTumbling(seconds) => s"win:time_batch($seconds)"
  }

  val window1Epl: String = getEplFrom(join.window1)
  val window2Epl: String = getEplFrom(join.window2)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery.$window1Epl as lhs, subquery.$window2Epl as rhs")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val lhsElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("lhs").asInstanceOf[Array[AnyRef]]
        val rhsElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("rhs").asInstanceOf[Array[AnyRef]]
        val lhsAndRhsElementValues: Array[AnyRef] = lhsElementValues ++ rhsElementValues
        val lhsAndRhsElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
        val event: Event = Event.getEventFrom(clock.instant, lhsAndRhsElementValues, lhsAndRhsElementClasses)
        context.parent ! event
      }
    }
  })

  val subqueryNode: ActorRef = Node.createChildNodeFrom(join.subquery1, nodeName, 1, publishers, context)

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
  }

}
