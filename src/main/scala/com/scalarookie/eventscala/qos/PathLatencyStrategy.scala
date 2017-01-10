package com.scalarookie.eventscala.qos

import java.time.{Clock, Duration, Instant}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{ActorContext, ActorRef}
import com.scalarookie.eventscala.caseclasses._

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, duration: Duration)

class PathLatencyLeafNodeStrategy extends LeafNodeQosStrategy {

  override def onMessageReceive(message: Any, data: LeafNodeData): Unit = message match {
    case ChildLatencyRequest(requestTime) =>
      data.context.parent ! ChildLatencyResponse(data.context.self, requestTime)
      data.context.parent ! PathLatency(data.context.self, Duration.ZERO)
  }

}

class PathLatencyUnaryNodeStrategy(interval: Int) extends UnaryNodeQosStrategy {

  val clock: Clock = Clock.systemDefaultZone
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None

  override def onCreated(nodeData: UnaryNodeData): Unit = {
    nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = new Runnable {
        override def run(): Unit = {
          nodeData.childNode ! ChildLatencyRequest(clock.instant)
        }
      }
    )
  }

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = message match {
    case ChildLatencyRequest(time) =>
      nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
    case ChildLatencyResponse(_, requestTime) =>
      childNodeLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (childNodePathLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
        /* TODO */ println(s"===> ${nodeData.name} path latency: $pathLatency")
        childNodeLatency = None
        childNodePathLatency = None
      }
    case PathLatency(_, duration) =>
      childNodePathLatency = Some(duration)
      if (childNodeLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
        /* TODO */ println(s"===> ${nodeData.name} path latency: $pathLatency")
        childNodeLatency = None
        childNodePathLatency = None
      }
  }
}

class PathLatencyBinaryNodeStrategy(interval: Int) extends BinaryNodeQosStrategy {
  val clock: Clock = Clock.systemDefaultZone
  var childNode1Latency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  override def onCreated(nodeData: BinaryNodeData): Unit = {
    nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = new Runnable {
        override def run(): Unit = {
          nodeData.childNode1 ! ChildLatencyRequest(clock.instant)
          nodeData.childNode2 ! ChildLatencyRequest(clock.instant)
        }
      }
    )
  }

  override def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = message match {
    case ChildLatencyRequest(time) =>
      nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
    case ChildLatencyResponse(childNode, requestTime) =>
      childNode match {
        case nodeData.childNode1 => childNode1Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
        case nodeData.childNode2 => childNode2Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      }
      if (childNode1Latency.isDefined &&
          childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined &&
          childNode2PathLatency.isDefined) {
        val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
        val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
        if (pathLatency1.compareTo(pathLatency2) >= 0) {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency1)
          /* TODO */ println(s"===> ${nodeData.name} path latency: $pathLatency1")
        } else {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
          /* TODO */ println(s"===> ${nodeData.name} path latency: $pathLatency2")
        }
        childNode1Latency = None
        childNode2Latency = None
        childNode1PathLatency = None
        childNode2PathLatency = None
      }
    case PathLatency(childNode, duration) =>
      childNode match {
        case nodeData.childNode1 => childNode1PathLatency = Some(duration)
        case nodeData.childNode2 => childNode2PathLatency = Some(duration)
      }
      if (childNode1Latency.isDefined &&
          childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined &&
          childNode2PathLatency.isDefined) {
        val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
        val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
        if (pathLatency1.compareTo(pathLatency2) >= 0) {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency1)
          /* TODO */ println(s"===> ${nodeData.name} path latency: $pathLatency1")
        } else {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
          /* TODO */ println(s"===> ${nodeData.name} path latency: $pathLatency2")
        }
        childNode1Latency = None
        childNode2Latency = None
        childNode1PathLatency = None
        childNode2PathLatency = None
      }
  }
}
