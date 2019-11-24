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

case class EvaluationPublisher(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name

  var id = 0

  var recordOnce = false
  var begin: Long = 0


  def publish(id: Int): Unit = {
    val event: Event = createEventFromId(id)
    if(!recordOnce){
      begin = System.nanoTime()
      recordOnce = true
    }
//    event match {
//      case Event1(e1: MeasureEvent) =>
//        if(id == 5000) recordOnce = true
//        if(!recordOnce){
//          val timestamp = System.nanoTime()
//          publishedEventsTimestamps.put(e1.id, timestamp)
//        }
//    }
    source._1.offer(event)
  }

//  val publishedEventsTimestamps: Map[String, Long] = Map.empty[String, Long]

  context.system.scheduler.schedule(
    initialDelay = FiniteDuration(20000, TimeUnit.MILLISECONDS),
    interval = FiniteDuration(1, TimeUnit.SECONDS),
    runnable = () => {
      (1 to 5000).foreach(n => {
//        Thread.sleep(200)
        publish(n)
      })
    })

  override def receive: Receive = {
    case Subscribe =>
      super.receive(Subscribe)
    case EventReceived(eid,data) =>
      val t = System.nanoTime()
      val timeSpan = t / 1000000
      println(s"$data,$timeSpan,$begin")
//      if (publishedEventsTimestamps.contains(eid)) {
//        val t0 = publishedEventsTimestamps(eid)
//        val t1 = System.nanoTime()
//        val timespan = (t1 - t0) / 1000000
//        println(s"${eid},${data},${timespan}")
//      }
//      else {
//        println("did not record this event!")
//      }
  }


}
