package com.scalarookie.eventscala.actors

import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.actors.PublisherActor.Subscribe

class StreamActor(stream: Stream, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  /* TODO DELETE */ println(s"Node ${self.path.name} created; representing $stream.")

  // Obviously, `StreamActor` has to subscribe to the respective publisher.
  publishers(stream.name) ! Subscribe

  override def receive: Receive = {
    case event: Event if sender.path.name == stream.name =>
      // If this node is the root of the graph, we print the new event to the console, otherwise we pass it up.
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
  }
}
