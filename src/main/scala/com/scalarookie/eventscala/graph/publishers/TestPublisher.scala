package com.scalarookie.eventscala.graph.publishers

import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.graph.publishers.PublisherActor._

case class TestPublisher() extends PublisherActor {

  override def receive: Receive = {
    case Subscribe =>
      subscribers = subscribers + sender
    case Unsubscribe =>
      subscribers = subscribers - sender
    case message =>
      subscribers.foreach(_ ! message)
  }

}