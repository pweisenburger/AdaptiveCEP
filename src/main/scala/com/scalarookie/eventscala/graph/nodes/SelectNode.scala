package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{StrategyFactory, UnaryNodeStrategy}

class SelectNode(select: Select,
                 frequencyStrategyFactory: StrategyFactory,
                 latencyStrategyFactory: StrategyFactory,
                 publishers: Map[String, ActorRef],
                 callbackIfRoot: Option[Event => Any] = None)
  extends UnaryNode(select,
                    frequencyStrategyFactory,
                    latencyStrategyFactory,
                    publishers,
                    callbackIfRoot) {

  val elementIdsEpl: String = select.elementIds.map(i => s"sq.e$i").mkString(", ")

  val eplString: String = s"select $elementIdsEpl from subquery as sq"

  def event2EventBean(eventBean: EventBean): Event = {
    val subqueryElementValues: Array[AnyRef] = select.elementIds.map(i => s"sq.e$i").map(eventBean.get).toArray
    val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(select)
    Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
  }

  createEplStatementAndAddListener(eplString, event2EventBean)

}
