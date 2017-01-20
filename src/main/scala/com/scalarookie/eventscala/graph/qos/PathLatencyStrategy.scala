package com.scalarookie.eventscala.graph.qos

import java.time.{Clock, Duration, Instant}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorRef
import com.scalarookie.eventscala.caseclasses._

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, duration: Duration)

trait PathLatencyStrategy {

  def latencyRequirementDefinedAndNotMet(latency: Duration, latencyRequirement: Option[LatencyRequirement]): Boolean = {
    if (latencyRequirement.isDefined) {
      val met: Boolean = latencyRequirement.get.operator match {
        case Equal        => latency.compareTo(latencyRequirement.get.duration) == 0
        case NotEqual     => latency.compareTo(latencyRequirement.get.duration) != 0
        case Greater      => latency.compareTo(latencyRequirement.get.duration) >  0
        case GreaterEqual => latency.compareTo(latencyRequirement.get.duration) >= 0
        case Smaller      => latency.compareTo(latencyRequirement.get.duration) <  0
        case SmallerEqual => latency.compareTo(latencyRequirement.get.duration) <= 0
      }
      !met
    } else {
      false
    }
  }

}

class PathLatencyLeafNodeStrategy extends PathLatencyStrategy with LeafNodeStrategy {

  override def onMessageReceive(message: Any, data: LeafNodeData): Unit = message match {
    case ChildLatencyRequest(requestTime) =>
      data.context.parent ! ChildLatencyResponse(data.context.self, requestTime)
      data.context.parent ! PathLatency(data.context.self, Duration.ZERO)
  }

}

class PathLatencyUnaryNodeStrategy(interval: Int, logging: Boolean) extends PathLatencyStrategy with UnaryNodeStrategy {

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
        if (logging) println(s"LATENCY LOG:\t\t${nodeData.name}: $pathLatency")
        if (latencyRequirementDefinedAndNotMet(pathLatency, nodeData.query.latencyRequirement)) {
          nodeData.query.latencyRequirement.get.callback(nodeData.name)
        }
        childNodeLatency = None
        childNodePathLatency = None
      }
    case PathLatency(_, duration) =>
      childNodePathLatency = Some(duration)
      if (childNodeLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
        if (logging) println(s"LATENCY LOG:\t\t${nodeData.name}: $pathLatency")
        if (latencyRequirementDefinedAndNotMet(pathLatency, nodeData.query.latencyRequirement)) {
          nodeData.query.latencyRequirement.get.callback(nodeData.name)
        }
        childNodeLatency = None
        childNodePathLatency = None
      }
  }

}

class PathLatencyBinaryNodeStrategy(interval: Int, logging: Boolean) extends PathLatencyStrategy with BinaryNodeStrategy {

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
          if (logging) println(s"LATENCY LOG:\t\t${nodeData.name}: $pathLatency1")
          if (latencyRequirementDefinedAndNotMet(pathLatency1, nodeData.query.latencyRequirement)) {
            nodeData.query.latencyRequirement.get.callback(nodeData.name)
          }
        } else {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
          if (logging) println(s"LATENCY LOG:\t\t${nodeData.name}: $pathLatency2")
          if (latencyRequirementDefinedAndNotMet(pathLatency2, nodeData.query.latencyRequirement)) {
            nodeData.query.latencyRequirement.get.callback(nodeData.name)
          }
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
          if (logging) println(s"LATENCY LOG:\t\t${nodeData.name}: $pathLatency1")
          if (latencyRequirementDefinedAndNotMet(pathLatency1, nodeData.query.latencyRequirement)) {
            nodeData.query.latencyRequirement.get.callback(nodeData.name)
          }
        } else {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
          if (logging) println(s"LATENCY LOG:\t\t${nodeData.name}: $pathLatency2")
          if (latencyRequirementDefinedAndNotMet(pathLatency2, nodeData.query.latencyRequirement)) {
            nodeData.query.latencyRequirement.get.callback(nodeData.name)
          }
        }
        childNode1Latency = None
        childNode2Latency = None
        childNode1PathLatency = None
        childNode2PathLatency = None
      }
  }

}

case class PathLatencyStrategyFactory(interval: Int, logging: Boolean) extends StrategyFactory {

  override def getLeafNodeStrategy: LeafNodeStrategy = new PathLatencyLeafNodeStrategy
  override def getUnaryNodeStrategy: UnaryNodeStrategy = new PathLatencyUnaryNodeStrategy(interval, logging)
  override def getBinaryNodeStrategy: BinaryNodeStrategy = new PathLatencyBinaryNodeStrategy(interval, logging)

}
