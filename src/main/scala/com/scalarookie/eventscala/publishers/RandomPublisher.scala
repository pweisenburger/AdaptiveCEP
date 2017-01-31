package com.scalarookie.eventscala.publishers

import java.util.concurrent.TimeUnit
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import com.scalarookie.eventscala.data.Events._

case class RandomPublisher(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name

  def publish(id: Integer): Unit = {
    val event: Event = createEventFromId(id)
    subscribers.foreach(_ ! event)
    println(s"STREAM $publisherName:\t$event")
    context.system.scheduler.scheduleOnce(
      delay = FiniteDuration(Random.nextInt(5000), TimeUnit.MILLISECONDS),
      runnable = () => publish(id + 1)
    )
  }

  publish(0)

}
