package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

class SelectNode(select: Select, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyUnaryNodeStrategy)
  extends UnaryNode(select.subquery, select.frequencyRequirement, frequencyStrategy, select.latencyRequirement, latencyStrategy, publishers) {

  val elementIdsEpl: String = select.elementIds.map(i => s"sq.e$i").mkString(", ")

  val eplString: String = s"select $elementIdsEpl from subquery as sq"

  def event2EventBean(eventBean: EventBean): Event = {
    val subqueryElementValues: Array[AnyRef] = select.elementIds.map(i => s"sq.e$i").map(eventBean.get).toArray
    val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(select)
    Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
  }

  createEplStatementAndAddListener(eplString, event2EventBean)
}
