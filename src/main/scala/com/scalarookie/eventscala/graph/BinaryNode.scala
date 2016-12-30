package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client.{EventBean, UpdateListener}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyBinaryNodeStrategy}

abstract class BinaryNode(lhsSubquery: Query,
                          rhsSubquery: Query,
                          frequencyRequirement: Option[FrequencyRequirement],
                          frequencyStrategy: FrequencyStrategy,
                          latencyRequirement: Option[LatencyRequirement],
                          latencyStrategy: PathLatencyBinaryNodeStrategy,
                          publishers: Map[String, ActorRef])
  extends Actor with EsperEngine {

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subquery1ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(lhsSubquery)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(rhsSubquery)
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses)
  addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses)

  val subquery1Node: ActorRef = Node.createChildNodeFrom(lhsSubquery, nodeName, 1, publishers, context)
  val subquery2Node: ActorRef = Node.createChildNodeFrom(rhsSubquery, nodeName, 2, publishers, context)

  var oneChildCreated = false

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
    case event: Event if sender == subquery1Node =>
      sendEvent("subquery1", Event.getArrayOfValuesFrom(event))
    case event: Event if sender == subquery2Node =>
      sendEvent("subquery2", Event.getArrayOfValuesFrom(event))
    case Created =>
      if (oneChildCreated) {
        context.parent ! Created
        if (frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, frequencyRequirement.get)
        latencyStrategy.onSubtreeCreated(self, subquery1Node, subquery2Node, context, nodeName, latencyRequirement)
      } else {
        oneChildCreated = true
      }
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, self, subquery1Node, subquery2Node, context, nodeName, latencyRequirement)
  }
}
