package com.scalarookie.eventscala.graph.publishers

import akka.actor.{Actor, ActorRef}
import PublisherActor._

object PublisherActor {

  case object Subscribe
  case object AckSubscription

}

trait PublisherActor extends Actor {

  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty[ActorRef]

  override def receive: Receive = {
    case Subscribe =>
      subscribers = subscribers + sender()
      sender() ! AckSubscription
  }

}
