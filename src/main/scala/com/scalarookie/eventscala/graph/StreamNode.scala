package com.scalarookie.eventscala.graph

import java.time.{Clock, Instant}
import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.publishers.PublisherActor.Subscribe

class StreamNode(stream: Stream, publishers: Map[String, ActorRef]) extends Actor {

  // TODO Experimental!
  val clock: Clock = Clock.systemDefaultZone

  val publisher: ActorRef = publishers(stream.name)

  publisher ! Subscribe

  // TODO Experimental!
  def updateTimestampOf(event: Event, timestamp: Instant): Event = event match {
    case Event1(_, tuple) => Event1(timestamp, tuple)
    case Event2(_, tuple) => Event2(timestamp, tuple)
    case Event3(_, tuple) => Event3(timestamp, tuple)
    case Event4(_, tuple) => Event4(timestamp, tuple)
    case Event5(_, tuple) => Event5(timestamp, tuple)
    case Event6(_, tuple) => Event6(timestamp, tuple)
  }

  override def receive: Receive = {
    case event: Event if sender == publisher =>
      context.parent ! updateTimestampOf(event, clock.instant)
  }

}
