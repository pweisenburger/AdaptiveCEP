package com.scalarookie.eventscala.actors

import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.actors.PublisherActor.Subscribe

class StreamActor(stream: Stream, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  /* TODO */ println(s"Node `${self.path.name}` created; representing `$stream`.")

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
  }

}
