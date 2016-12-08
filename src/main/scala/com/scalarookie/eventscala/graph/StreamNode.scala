package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.publishers.PublisherActor._

class StreamNode(stream: Stream, publishers: Map[String, ActorRef]) extends Actor {

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      context.parent ! event
    /******************************************************************************************************************/
    case LatencyRequest(time) =>
      sender ! LatencyResponse(time)
    /******************************************************************************************************************/
  }

}
