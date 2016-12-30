package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

class SelfJoinNode(join: Join, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyUnaryNodeStrategy)
  extends UnaryNode(join.subquery1, join.frequencyRequirement, frequencyStrategy, join.latencyRequirement, latencyStrategy, publishers) {

  require(join.subquery1 == join.subquery2)

  val window1Epl: String = JoinNode.getEplFrom(join.window1)
  val window2Epl: String = JoinNode.getEplFrom(join.window2)

  val eplString: String = s"select * from subquery.$window1Epl as lhs, subquery.$window2Epl as rhs"

  def eventBean2Event(eventBean: EventBean): Event = {
    val lhsElementValues: Array[AnyRef] = eventBean.get("lhs").asInstanceOf[Array[AnyRef]]
    val rhsElementValues: Array[AnyRef] = eventBean.get("rhs").asInstanceOf[Array[AnyRef]]
    val lhsAndRhsElementValues: Array[AnyRef] = lhsElementValues ++ rhsElementValues
    val lhsAndRhsElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
    Event.getEventFrom(lhsAndRhsElementValues, lhsAndRhsElementClasses)
  }

  createEplStatementAndAddListener(eplString, eventBean2Event)
}
