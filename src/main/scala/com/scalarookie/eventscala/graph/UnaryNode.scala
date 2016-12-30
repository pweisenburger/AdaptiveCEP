package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client.{EventBean, UpdateListener}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

abstract class UnaryNode(subquery: Query,
                         frequencyRequirement: Option[FrequencyRequirement],
                         frequencyStrategy: FrequencyStrategy,
                         latencyRequirement: Option[LatencyRequirement],
                         latencyStrategy: PathLatencyUnaryNodeStrategy,
                         publishers: Map[String, ActorRef])
extends Actor with EsperEngine {



  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val subqueryNode: ActorRef = Node.createChildNodeFrom(subquery, nodeName, 1, publishers, context)

  def createEplStatementAndAddListener(eplString: String, eventBean2Event: EventBean => Event): Unit =
    createEplStatement(eplString).addListener(new UpdateListener {
      override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
        val events: List[Event] = newEvents.map(eventBean2Event).toList
        events.foreach(event => {
          if (frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, frequencyRequirement.get)
          context.parent ! event
        })
      }
    })

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
    case Created =>
      context.parent ! Created
      if (frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, frequencyRequirement.get)
      latencyStrategy.onSubtreeCreated(self, subqueryNode, context, nodeName, latencyRequirement)
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, self, subquery, subqueryNode, context, nodeName, latencyRequirement)
  }
}
