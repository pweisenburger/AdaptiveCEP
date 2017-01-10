package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos._
import com.scalarookie.eventscala.publishers.PublisherActor._

class StreamNode(stream: Stream,
                 publishers: Map[String, ActorRef],
                 frequencyStrategy: LeafNodeStrategy,
                 latencyStrategy: LeafNodeStrategy)
  extends Node(publishers) {


  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  context.parent ! Created

  val nodeData: LeafNodeData = LeafNodeData(stream.name, stream, context)

  frequencyStrategy.onCreated(nodeData)
  latencyStrategy.onCreated(nodeData)

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      context.parent ! event
      frequencyStrategy.onEventEmit(event, nodeData)
      latencyStrategy.onEventEmit(event, nodeData)
    case unhandledMessage =>
      frequencyStrategy.onMessageReceive(unhandledMessage, nodeData)
      latencyStrategy.onMessageReceive(unhandledMessage, nodeData)
  }

}
