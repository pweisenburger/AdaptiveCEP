package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyLeafNodeStrategy}
import com.scalarookie.eventscala.publishers.PublisherActor._

class StreamNode(stream: Stream, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyLeafNodeStrategy) extends Actor {

  val nodeName: String = self.path.name

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  context.parent ! Created

  if (stream.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, stream.frequencyRequirement.get)

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      context.parent ! event
      if (stream.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, stream.frequencyRequirement.get)
    /*case LatencyRequest(time) =>
      sender ! LatencyResponse(time)*/
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, context)
  }

}
