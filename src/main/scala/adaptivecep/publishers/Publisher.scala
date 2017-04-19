package adaptivecep.publishers

import akka.actor.{Actor, ActorRef}
import Publisher._

trait Publisher extends Actor {

  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty[ActorRef]

  override def receive: Receive = {
    case Subscribe =>
      subscribers = subscribers + sender()
      sender() ! AcknowledgeSubscription
  }

}

object Publisher {

  case object Subscribe
  case object AcknowledgeSubscription

}
