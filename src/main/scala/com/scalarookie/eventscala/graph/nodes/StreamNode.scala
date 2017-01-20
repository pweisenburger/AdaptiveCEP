package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.graph.publishers.PublisherActor._
import com.scalarookie.eventscala.graph.qos._

class StreamNode(stream: Stream,
                 frequencyStrategyFactory: StrategyFactory,
                 latencyStrategyFactory: StrategyFactory,
                 publishers: Map[String, ActorRef],
                 callbackIfRoot: Option[Event => Any] = None)
  extends Node(publishers) {

  val frequencyStrategy: LeafNodeStrategy = frequencyStrategyFactory.getLeafNodeStrategy
  val latencyStrategy: LeafNodeStrategy = latencyStrategyFactory.getLeafNodeStrategy

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  context.parent ! Created

  val nodeData: LeafNodeData = LeafNodeData(stream.name, stream, context)

  frequencyStrategy.onCreated(nodeData)
  latencyStrategy.onCreated(nodeData)

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(event) else context.parent ! event
      frequencyStrategy.onEventEmit(event, nodeData)
      latencyStrategy.onEventEmit(event, nodeData)
    case unhandledMessage =>
      frequencyStrategy.onMessageReceive(unhandledMessage, nodeData)
      latencyStrategy.onMessageReceive(unhandledMessage, nodeData)
  }

}
