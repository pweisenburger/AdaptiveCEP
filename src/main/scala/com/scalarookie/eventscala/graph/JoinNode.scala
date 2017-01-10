package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{BinaryNodeStrategy, LatencyBinaryNodeStrategy}

object JoinNode {

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(instances) => s"win:length($instances)"
    case LengthTumbling(instances) => s"win:length_batch($instances)"
    case TimeSliding(seconds) => s"win:time($seconds)"
    case TimeTumbling(seconds) => s"win:time_batch($seconds)"
  }

}

class JoinNode(join: Join,
               publishers: Map[String, ActorRef],
               frequencyStrategy: BinaryNodeStrategy,
               latencyStrategy: BinaryNodeStrategy)
  extends BinaryNode(join,
                     frequencyStrategy,
                     latencyStrategy,
                     publishers) {

  val window1Epl: String = JoinNode.getEplFrom(join.window1)
  val window2Epl: String = JoinNode.getEplFrom(join.window2)

  val eplString: String = s"select * from subquery1.$window1Epl as sq1, subquery2.$window2Epl as sq2"

  def eventBean2Event(eventBean: EventBean): Event = {
    val subquery1ElementValues: Array[AnyRef] = eventBean.get("sq1").asInstanceOf[Array[AnyRef]]
    val subquery2ElementValues: Array[AnyRef] = eventBean.get("sq2").asInstanceOf[Array[AnyRef]]
    val subqueries1And2ElementValues: Array[AnyRef] = subquery1ElementValues ++ subquery2ElementValues
    val subqueries1And2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
    Event.getEventFrom(subqueries1And2ElementValues, subqueries1And2ElementClasses)
  }

  createEplStatementAndAddListener(eplString, eventBean2Event)

}
