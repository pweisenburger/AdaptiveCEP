package adaptivecep.publishers

import java.util.concurrent.TimeUnit

import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import adaptivecep.data.Events._
import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}

case class RandomPublisher(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name

  var id = 0
  def publish(id: Int): Unit = {
    val event: Event = createEventFromId(id)
    source._1.offer(event)
  }

  context.system.scheduler.schedule(
    initialDelay = FiniteDuration(10000, TimeUnit.MILLISECONDS),
    interval = FiniteDuration(1, TimeUnit.SECONDS),
    runnable = () => {
      (1 to 5000).foreach(n => publish(n))
    })
}
