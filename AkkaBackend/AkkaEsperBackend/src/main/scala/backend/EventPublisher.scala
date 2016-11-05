package backend

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random
import akka.actor.{Actor, ActorRef}
import EventPublisher._

object EventPublisher {

  case object Subscribe
  case object Unsubscribe

}

trait EventPublisher extends Actor {

  var subscribers: Set[ActorRef] =
    scala.collection.immutable.Set.empty

  def receive: Receive = {
    case Subscribe =>
      this.subscribers = this.subscribers + sender
    case Unsubscribe =>
      this.subscribers = this.subscribers - sender
  }

}

case class RandomEventPublisher[T](createInstanceFromId: Int => T) extends EventPublisher {

  def publish(id: Int): Unit = {
    val t = createInstanceFromId(id)
    this.subscribers.foreach(_ ! t)
    println(s"Primitive event published:\t$t")
    context.system.scheduler.scheduleOnce(
      delay = FiniteDuration(Random.nextInt(5000), TimeUnit.MILLISECONDS),
      runnable = new Runnable { def run() = publish(id + 1) }
    )
  }

  this.publish(0)

}
