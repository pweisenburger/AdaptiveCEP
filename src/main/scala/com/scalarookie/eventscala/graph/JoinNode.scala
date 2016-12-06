package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

object JoinNode {

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(instances) => s"win:length($instances)"
    case LengthTumbling(instances) => s"win:length_batch($instances)"
    case TimeSliding(seconds) => s"win:time($seconds)"
    case TimeTumbling(seconds) => s"win:time_batch($seconds)"
  }

}

class JoinNode(join: Join, publishers: Map[String, ActorRef]) extends Actor with EsperEngine {

  require(join.subquery1 != join.subquery2)

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subquery1ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery1)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery2)
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses)
  addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses)

  val window1Epl: String = JoinNode.getEplFrom(join.window1)
  val window2Epl: String = JoinNode.getEplFrom(join.window2)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery1.$window1Epl as sq1, subquery2.$window2Epl as sq2")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val subquery1ElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("sq1").asInstanceOf[Array[AnyRef]]
        val subquery2ElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("sq2").asInstanceOf[Array[AnyRef]]
        val subqueries1And2ElementValues: Array[AnyRef] = subquery1ElementValues ++ subquery2ElementValues
        val subqueries1And2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
        val event: Event = Event.getEventFrom(subqueries1And2ElementValues, subqueries1And2ElementClasses)
        context.parent ! event
      }
    }
  })

  val subquery1Node: ActorRef = Node.createChildNodeFrom(join.subquery1, nodeName, 1, publishers, context)
  val subquery2Node: ActorRef = Node.createChildNodeFrom(join.subquery2, nodeName, 2, publishers, context)

  override def receive: Receive = {
    case event: Event if sender == subquery1Node =>
      sendEvent("subquery1", Event.getArrayOfValuesFrom(event))
    case event: Event if sender == subquery2Node =>
      sendEvent("subquery2", Event.getArrayOfValuesFrom(event))
  }

}
