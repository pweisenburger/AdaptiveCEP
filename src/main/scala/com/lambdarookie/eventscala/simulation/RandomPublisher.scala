package com.lambdarookie.eventscala.simulation

import java.util.concurrent.TimeUnit

import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.publishers.Publisher

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

case class RandomPublisher(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name

  def publish(id: Integer): Unit = {
    val event: Event = createEventFromId(id)
    subscribers.foreach(_ ! event)
    println(s"STREAM $publisherName:\t$event")
    context.system.scheduler.scheduleOnce(
      delay = FiniteDuration(Random.nextInt(3000), TimeUnit.MILLISECONDS),
      runnable = () => publish(id + 1)
    )
  }

  publish(0)

}
