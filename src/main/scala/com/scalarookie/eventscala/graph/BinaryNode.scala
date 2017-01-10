package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.espertech.esper.client.{EventBean, UpdateListener}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{BinaryNodeData, FrequencyStrategy, PathLatencyBinaryNodeStrategy}

abstract class BinaryNode(query: BinaryQuery,
                          frequencyStrategy: FrequencyStrategy,
                          latencyStrategy: PathLatencyBinaryNodeStrategy,
                          publishers: Map[String, ActorRef])
  extends Node(publishers) with EsperEngine {

  override val esperServiceProviderUri: String = nodeName

  val subquery1ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(query.subquery1)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(query.subquery2)
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses)
  addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses)

  val subquery1Node: ActorRef = createChildNode(query.subquery1, 1)
  val subquery2Node: ActorRef = createChildNode(query.subquery2, 2)

  var oneChildCreated = false

  val nodeData: BinaryNodeData = BinaryNodeData(nodeName, query, context, subquery1Node, subquery2Node)

  def createEplStatementAndAddListener(eplString: String, eventBean2Event: EventBean => Event): Unit =
    createEplStatement(eplString).addListener(new UpdateListener {
      override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
        val events: List[Event] = newEvents.map(eventBean2Event).toList
        events.foreach(event => {
          if (query.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, query.frequencyRequirement.get)
          context.parent ! event
          latencyStrategy.onEventEmit(event, nodeData)
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
        if (query.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, query.frequencyRequirement.get)
        latencyStrategy.onCreated(nodeData)
      } else {
        oneChildCreated = true
      }
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, nodeData)
  }

}
