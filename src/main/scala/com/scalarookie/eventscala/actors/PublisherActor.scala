package com.scalarookie.eventscala.actors

import java.util.concurrent.TimeUnit
import scala.util.Random
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.actors.PublisherActor._

object PublisherActor {

  case object Subscribe
  case object Unsubscribe

}

trait PublisherActor extends Actor {

  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty

  override def receive: Receive = {
    case Subscribe =>
      this.subscribers = this.subscribers + sender
    case Unsubscribe =>
      this.subscribers = this.subscribers - sender
  }

}

case class RandomPublisherActor[T](createEventFromId: Integer => T) extends PublisherActor {

  def publish(id: Integer): Unit = {
    val t = createEventFromId(id)
    this.subscribers.foreach(_ ! t)
    println(s"Published in stream ${self.path.name}: $t")
    context.system.scheduler.scheduleOnce(
      delay = FiniteDuration(Random.nextInt(5000), TimeUnit.MILLISECONDS),
      runnable = new Runnable { def run() = publish(id + 1) }
    )
  }

  this.publish(0)

}