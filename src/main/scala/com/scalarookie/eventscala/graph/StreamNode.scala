package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos._
import com.scalarookie.eventscala.publishers.PublisherActor._

class StreamNode(stream: Stream,
                 publishers: Map[String, ActorRef],
                 frequencyStrategy: FrequencyStrategy,
                 latencyStrategy: LeafNodeQosStrategy)
  extends Node(publishers) {

  val data: LeafNodeData = LeafNodeData(stream.name, stream, context)

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  context.parent ! Created

  if (stream.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, stream.frequencyRequirement.get)
  latencyStrategy.onCreated(data)

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      context.parent ! event
      if (stream.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, stream.frequencyRequirement.get)
      latencyStrategy.onEventEmit(event, data)
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, data)
  }

}
