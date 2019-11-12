package adaptivecep.publishers

import java.util.concurrent.TimeUnit

import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import adaptivecep.data.Events._
import adaptivecep.publishers.Publisher.Subscribe
import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}

import scala.collection.mutable.Map

case class EvaluationPublisher(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name

  var id = 0
  def publish(id: Int): Unit = {
    val event: Event = createEventFromId(id)
    source._1.offer(event)
  }

  val publishedEventsTimestamps: Map[Int,Long] = Map.empty[Int,Long]

  (1 to 5000).foreach(n => {
    val timestamp = System.nanoTime()
    if(!publishedEventsTimestamps.contains(n))
      publishedEventsTimestamps.put(n,timestamp)
    publish(n)
  })

//  context.system.scheduler.schedule(
//    initialDelay = FiniteDuration(0, TimeUnit.MILLISECONDS),
//    interval = FiniteDuration(1, TimeUnit.SECONDS),
//    runnable = () => {
//      (1 to 5000).foreach(n => {
//        val timestamp = System.nanoTime()
//        if(!publishedEventsTimestamps.contains(n))
//          publishedEventsTimestamps.put(n,timestamp)
//        publish(n)
//      })
//    })

  override def receive: Receive = {
    case Subscribe =>
      super.receive(Subscribe)
    case EventReceived(eid: Int) =>
    if(publishedEventsTimestamps.contains(eid)){
      val t0 = publishedEventsTimestamps(eid)
      val t1 = System.nanoTime()
      val timespan = (t1 - t0) / 1000000
      println(s"${eid},${timespan}")
    }else{
      println("didn't send this event!")
    }
  }


}
