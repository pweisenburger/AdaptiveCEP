package com.scalarookie.eventscala.qos

import java.time.{Clock, Duration, Instant}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{ActorContext, ActorRef}
import com.scalarookie.eventscala.caseclasses.{LatencyRequirement, Query}

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, duration: Duration)

class PathLatencyLeafNodeStrategy extends LatencyStrategy {
  def onMessageReceive(message: Any, context: ActorContext): Unit = message match {
    case ChildLatencyRequest(requestTime) =>
      context.parent ! ChildLatencyResponse(context.self, requestTime)
      context.parent ! PathLatency(context.self, Duration.ZERO)
  }
}

class PathLatencyUnaryNodeStrategy(interval: Int) extends LatencyStrategy {
  val clock: Clock = Clock.systemDefaultZone
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None

  def onSubtreeCreated(self: ActorRef, childNode: ActorRef, context: ActorContext, nodeName: String, latencyRequirement: Option[LatencyRequirement]): Unit = {
    context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = new Runnable {
        override def run(): Unit = {
          childNode ! ChildLatencyRequest(clock.instant)
        }
      }
    )
  }

  def onMessageReceive(message: Any, self: ActorRef, subquery: Query, childNode: ActorRef, context: ActorContext, nodeName: String, latencyRequirement: Option[LatencyRequirement]): Unit = message match {
    case ChildLatencyRequest(time) =>
      context.parent ! ChildLatencyResponse(context.self, time)
    case ChildLatencyResponse(_, requestTime) =>
      childNodeLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (childNodePathLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        context.parent ! PathLatency(context.self, pathLatency)
        /* TODO */ println(s"===> $nodeName path latency: $pathLatency")
        childNodeLatency = None
        childNodePathLatency = None
      }
    case PathLatency(_, duration) =>
      childNodePathLatency = Some(duration)
      if (childNodeLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        context.parent ! PathLatency(context.self, pathLatency)
        /* TODO */ println(s"===> $nodeName path latency: $pathLatency")
        childNodeLatency = None
        childNodePathLatency = None
      }
  }
}

class PathLatencyBinaryNodeStrategy(interval: Int) extends LatencyStrategy {
  val clock: Clock = Clock.systemDefaultZone
  var childNode1Latency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  def onSubtreeCreated(self: ActorRef, childNode1: ActorRef, childNode2: ActorRef, context: ActorContext, nodeName: String, latencyRequirement: Option[LatencyRequirement]): Unit = {
    context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = new Runnable {
        override def run(): Unit = {
          childNode1 ! ChildLatencyRequest(clock.instant)
          childNode2 ! ChildLatencyRequest(clock.instant)
        }
      }
    )
  }

  def onMessageReceive(message: Any, self: ActorRef, childNode1: ActorRef, childNode2: ActorRef, context: ActorContext, nodeName: String, latencyRequirement: Option[LatencyRequirement]): Unit = message match {
    case ChildLatencyRequest(time) =>
      context.parent ! ChildLatencyResponse(context.self, time)
    case ChildLatencyResponse(childNode, requestTime) =>
      childNode match {
        case `childNode1` => childNode1Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
        case `childNode2` => childNode2Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      }
      if (childNode1Latency.isDefined &&
          childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined &&
          childNode2PathLatency.isDefined) {
        val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
        val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
        if (pathLatency1.compareTo(pathLatency2) >= 0) {
          context.parent ! PathLatency(context.self, pathLatency1)
          /* TODO */ println(s"===> $nodeName path latency: $pathLatency1")
        } else {
          context.parent ! PathLatency(context.self, pathLatency2)
          /* TODO */ println(s"===> $nodeName path latency: $pathLatency2")
        }
        childNode1Latency = None
        childNode2Latency = None
        childNode1PathLatency = None
        childNode2PathLatency = None
      }
    case PathLatency(childNode, duration) =>
      childNode match {
        case `childNode1` => childNode1PathLatency = Some(duration)
        case `childNode2` => childNode2PathLatency = Some(duration)
      }
      if (childNode1Latency.isDefined &&
        childNode2Latency.isDefined &&
        childNode1PathLatency.isDefined &&
        childNode2PathLatency.isDefined) {
        val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
        val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
        if (pathLatency1.compareTo(pathLatency2) >= 0) {
          context.parent ! PathLatency(context.self, pathLatency1)
          /* TODO */ println(s"===> $nodeName path latency: $pathLatency1")
        } else {
          context.parent ! PathLatency(context.self, pathLatency2)
          /* TODO */ println(s"===> $nodeName path latency: $pathLatency2")
        }
        childNode1Latency = None
        childNode2Latency = None
        childNode1PathLatency = None
        childNode2PathLatency = None
      }
  }
}
