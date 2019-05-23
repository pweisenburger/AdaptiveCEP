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
    //println(id)
    source._1.offer(event)//subscribers.foreach(_ ! event)

    //subscribers.foreach(_ ! event)
    //println(s"STREAM $publisherName:\t$event")
    /*context.system.scheduler.schedule(
      delay = FiniteDuration(1000, TimeUnit.MICROSECONDS),
      runnable = () => publish(id + 1)
    )*/

  }

  //publish(0)

  context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.MILLISECONDS),
    interval = FiniteDuration(1, TimeUnit.SECONDS),
    runnable = () => {
      (1 to 5000).foreach(n => publish(n))
      //publish()
    })
}
