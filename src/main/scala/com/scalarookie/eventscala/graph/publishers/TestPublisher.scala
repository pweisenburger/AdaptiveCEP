package com.scalarookie.eventscala.graph.publishers

import com.scalarookie.eventscala.graph.publishers.PublisherActor._

case class TestPublisher() extends PublisherActor {

  override def receive: Receive = {
    case Subscribe =>
      subscribers = subscribers + sender()
      sender() ! AckSubscription
    case message =>
      subscribers.foreach(_ ! message)
  }

}