package adaptivecep.publishers

import java.util.concurrent.TimeUnit

import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import adaptivecep.data.Events._
import adaptivecep.privacy.shared.Custom.MeasureEvent
import adaptivecep.publishers.Publisher.Subscribe
import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}

import scala.collection.mutable.Map

case class EvaluationPublisher2(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name

  var id = 0
  var countReceived = 0
  var firstEvent: Long = 0
  var lastEvent: Long = 0

  var beginning: Long = System.nanoTime()
  var recordOnce = false

  def publish(id: Int): Unit = {
    val event: Event = createEventFromId(id)
    event match {
      case Event1(eid: Int) =>
        if (eid == 5000) recordOnce = true
        if (!recordOnce) {
          val timestamp = System.nanoTime()
          val time = (timestamp - beginning - 20000) / 1000000
          if (eid == 1)
            firstEvent = time
          if (eid == 4999)
            lastEvent = time
        }
      case _ =>
    }
    source._1.offer(event)
  }

  context.system.scheduler.schedule(
    initialDelay = FiniteDuration(20000, TimeUnit.MILLISECONDS),
    interval = FiniteDuration(1, TimeUnit.SECONDS),
    runnable = () => {
      (1 to 5000).foreach(n => {
        publish(n)
      })
    })

  override def receive: Receive = {
    case Subscribe =>
      super.receive(Subscribe)
    case EventReceived(_, data) =>
      countReceived += 1
      if (countReceived < 3500) {
        val t1 = System.nanoTime()
        val timeSpan = (t1 - beginning - 20000) / 1000000
        println(s"$data,$timeSpan,$firstEvent,$lastEvent")
      }

  }


}
