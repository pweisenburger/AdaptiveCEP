package com.scalarookie.eventscala.publishers

import java.time.{Clock, Instant}
import java.util.concurrent.TimeUnit
import scala.util.Random
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

case class RandomPublisher[T](createEventFromId: (Instant, Integer) => T) extends PublisherActor {

  // TODO Experimental!
  val clock: Clock = Clock.systemDefaultZone

  val publisherName: String = self.path.name

  def publish(id: Integer): Unit = {
    val event = createEventFromId(clock.instant, id)
    subscribers.foreach(_ ! event)
    println(s"Published in stream $publisherName: $event")
    context.system.scheduler.scheduleOnce(
      delay = FiniteDuration(Random.nextInt(5000), TimeUnit.MILLISECONDS),
      runnable = new Runnable { override def run(): Unit = publish(id + 1) }
    )
  }

  publish(0)

}
