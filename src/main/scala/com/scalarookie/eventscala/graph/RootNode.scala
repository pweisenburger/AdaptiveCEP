package com.scalarookie.eventscala.graph

import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._

class RootNode(query: Query, publishers: Map[String, ActorRef], callback: Event => Any) extends Actor {

  val nodeName: String = self.path.name

  val childNode: ActorRef = Node.createChildNodeFrom(query, nodeName, 1, publishers, context)

  /********************************************************************************************************************/
  val clock: Clock = Clock.systemDefaultZone
  var childLatency: Option[Duration] = None
  var pathLatency: Option[Duration] = None
  context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(10, TimeUnit.SECONDS),
    runnable = new Runnable {
      override def run(): Unit = {
        childNode ! LatencyRequest(clock.instant)
      }
    })
  /********************************************************************************************************************/

  override def receive: Receive = {
    case event: Event if sender == childNode =>
      callback(event)
    /******************************************************************************************************************/
    case LatencyResponse(requestTime) =>
      childLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (childNode.isInstanceOf[Stream]) {
        pathLatency = Some(childLatency.get)
        /* TODO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
      }
    case PathLatency(duration) =>
      pathLatency = Some(duration.plus(childLatency.get))
      /* TODO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
    /******************************************************************************************************************/

  }

}
