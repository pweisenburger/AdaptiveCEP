package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.publishers.PublisherActor.Subscribe

class StreamNode(stream: Stream, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
  }

}
