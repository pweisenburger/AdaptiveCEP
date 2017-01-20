package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.graph.qos.{StrategyFactory, UnaryNodeStrategy}

class SelfJoinNode(selfJoin: SelfJoin,
                   frequencyStrategyFactory: StrategyFactory,
                   latencyStrategyFactory: StrategyFactory,
                   publishers: Map[String, ActorRef],
                   callbackIfRoot: Option[Event => Any] = None)
extends UnaryNode(selfJoin,
                  frequencyStrategyFactory,
                  latencyStrategyFactory,
                  publishers,
                  callbackIfRoot) {

  val window1Epl: String = JoinNode.getEplFrom(selfJoin.window1)
  val window2Epl: String = JoinNode.getEplFrom(selfJoin.window2)

  val eplString: String = s"select * from subquery.$window1Epl as lhs, subquery.$window2Epl as rhs"

  def eventBean2Event(eventBean: EventBean): Event = {
    val lhsElementValues: Array[AnyRef] = eventBean.get("lhs").asInstanceOf[Array[AnyRef]]
    val rhsElementValues: Array[AnyRef] = eventBean.get("rhs").asInstanceOf[Array[AnyRef]]
    val lhsAndRhsElementValues: Array[AnyRef] = lhsElementValues ++ rhsElementValues
    val lhsAndRhsElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(selfJoin)
    Event.getEventFrom(lhsAndRhsElementValues, lhsAndRhsElementClasses)
  }

  createEplStatementAndAddListener(eplString, eventBean2Event)

}
