package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.espertech.esper.client.{EventBean, UpdateListener}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

abstract class UnaryNode(query: UnaryQuery,
                         frequencyStrategy: FrequencyStrategy,
                         latencyStrategy: PathLatencyUnaryNodeStrategy,
                         publishers: Map[String, ActorRef])
  extends Node(publishers) with EsperEngine {

  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(query.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val subqueryNode: ActorRef = createChildNode(query.subquery, 1)

  def createEplStatementAndAddListener(eplString: String, eventBean2Event: EventBean => Event): Unit =
    createEplStatement(eplString).addListener(new UpdateListener {
      override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
        val events: List[Event] = newEvents.map(eventBean2Event).toList
        events.foreach(event => {
          if (query.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, query.frequencyRequirement.get)
          context.parent ! event
        })
      }
    })

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
    case Created =>
      context.parent ! Created
      if (query.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, query.frequencyRequirement.get)
      latencyStrategy.onSubtreeCreated(self, subqueryNode, context, nodeName, query.latencyRequirement)
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, self, query.subquery, subqueryNode, context, nodeName, query.latencyRequirement)
  }

}
