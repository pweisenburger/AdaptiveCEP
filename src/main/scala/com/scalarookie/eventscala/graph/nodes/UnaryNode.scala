package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client.{EventBean, UpdateListener}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.graph.qos.{StrategyFactory, UnaryNodeData, UnaryNodeStrategy}

abstract class UnaryNode(query: UnaryQuery,
                         frequencyStrategyFactory: StrategyFactory,
                         latencyStrategyFactory: StrategyFactory,
                         publishers: Map[String, ActorRef],
                         callbackIfRoot: Option[Event => Any])
  extends Node(publishers) with EsperEngine {

  val frequencyStrategy: UnaryNodeStrategy = frequencyStrategyFactory.getUnaryNodeStrategy
  val latencyStrategy: UnaryNodeStrategy = latencyStrategyFactory.getUnaryNodeStrategy

  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(query.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val subqueryNode: ActorRef = createChildNode(query.subquery, frequencyStrategyFactory, latencyStrategyFactory, 1)

  val nodeData: UnaryNodeData = UnaryNodeData(nodeName, query, context, subqueryNode)

  def createEplStatementAndAddListener(eplString: String, eventBean2Event: EventBean => Event): Unit =
    createEplStatement(eplString).addListener(new UpdateListener {
      override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
        val events: List[Event] = newEvents.map(eventBean2Event).toList
        events.foreach(event => {
          if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(event) else context.parent ! event
          frequencyStrategy.onEventEmit(event, nodeData)
          latencyStrategy.onEventEmit(event, nodeData)
        })
      }
    })

  override def receive: Receive = {
    case GraphCreated =>
      if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(GraphCreated) else context.parent ! GraphCreated
      frequencyStrategy.onCreated(nodeData)
      latencyStrategy.onCreated(nodeData)
    case event: Event if sender == subqueryNode =>
      sendEventToEsperEngine("subquery", Event.getArrayOfValuesFrom(event))
    case unhandledMessage =>
      frequencyStrategy.onMessageReceive(unhandledMessage, nodeData)
      latencyStrategy.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit =
    destroyEsperServiceProvider()

}
