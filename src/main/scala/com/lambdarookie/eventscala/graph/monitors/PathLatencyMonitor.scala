package com.lambdarookie.eventscala.graph.monitors

import java.time._
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries._

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, duration: Duration)

trait PathLatencyMonitor {

  def isRequirementNotMet(latency: Duration, lr: LatencyRequirement): Boolean = {
    val met: Boolean = lr.booleanOperator match {
      case Equal =>        latency.compareTo(lr.timeSpan.toDuration) == 0
      case NotEqual =>     latency.compareTo(lr.timeSpan.toDuration) != 0
      case Greater =>      latency.compareTo(lr.timeSpan.toDuration) >  0
      case GreaterEqual => latency.compareTo(lr.timeSpan.toDuration) >= 0
      case Smaller =>      latency.compareTo(lr.timeSpan.toDuration) <  0
      case SmallerEqual => latency.compareTo(lr.timeSpan.toDuration) <= 0
    }
    !met
  }

}

case class PathLatencyLeafNodeMonitor() extends PathLatencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = {
    if (nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
      println("PROBLEM:\tLatency requirements for leaf nodes are ignored, as leaf node latency is always considered 0.")
  }

  override def onMessageReceive(message: Any, data: LeafNodeData): Unit = message match {
    case ChildLatencyRequest(requestTime) =>
      data.context.parent ! ChildLatencyResponse(data.context.self, requestTime)
      data.context.parent ! PathLatency(data.context.self, Duration.ZERO)
  }

}

case class PathLatencyUnaryNodeMonitor(interval: Int, logging: Boolean, testing: Boolean)
  extends PathLatencyMonitor with UnaryNodeMonitor {

  val clock: Clock = Clock.systemDefaultZone
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None

  override def onCreated(nodeData: UnaryNodeData): Unit = nodeData.context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      nodeData.system.measureLatencies()
      nodeData.childNode ! ChildLatencyRequest(clock.instant)
    })

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = {
    val query: Query = nodeData.query
    val latencyRequirements: Set[LatencyRequirement] =
      query.requirements.collect { case lr: LatencyRequirement => lr }
    val system: System = nodeData.system
    val self: ActorRef = nodeData.context.self
    val operator: Operator = system.nodesToOperators.now.apply(self)
    message match {
      case ChildLatencyRequest(time) =>
        nodeData.context.parent ! ChildLatencyResponse(self, time)
      case ChildLatencyResponse(childNode, requestTime) =>
        if (testing) childNodeLatency = Some(operator.host.lastLatencies(system.getHostByNode(childNode)).toDuration)
        else childNodeLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))

        if (childNodePathLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          nodeData.context.parent ! PathLatency(self, pathLatency)
          if (logging && latencyRequirements.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathLatency.toMillis} ms. " +
              s"(Calculated every $interval seconds.)")
          latencyRequirements.foreach(lr =>
            if (isRequirementNotMet(pathLatency, lr)) query.addViolatedDemand(Violation(operator, lr)))
          childNodeLatency = None
          childNodePathLatency = None
        }
      case PathLatency(_, duration) =>
        childNodePathLatency = Some(duration)
        if (childNodeLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          nodeData.context.parent ! PathLatency(self, pathLatency)
          if (logging && latencyRequirements.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathLatency.toMillis} ms. " +
              s"(Calculated every $interval seconds.)")
          latencyRequirements.foreach(lr =>
            if (isRequirementNotMet(pathLatency, lr)) query.addViolatedDemand(Violation(operator, lr)))
          childNodeLatency = None
          childNodePathLatency = None
        }
    }
  }

}

case class PathLatencyBinaryNodeMonitor(interval: Int, logging: Boolean, testing: Boolean)
  extends PathLatencyMonitor with BinaryNodeMonitor {

  val clock: Clock = Clock.systemDefaultZone
  var childNode1Latency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  override def onCreated(nodeData: BinaryNodeData): Unit = nodeData.context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      nodeData.system.measureLatencies()
      nodeData.childNode1 ! ChildLatencyRequest(clock.instant)
      nodeData.childNode2 ! ChildLatencyRequest(clock.instant)
    })

  override def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = {
    val query = nodeData.query
    val latencyRequirements: Set[LatencyRequirement] =
      query.requirements.collect { case lr: LatencyRequirement => lr }
    val system: System = nodeData.system
    val self: ActorRef = nodeData.context.self
    val operator = system.nodesToOperators.now.apply(self)
    message match {
      case ChildLatencyRequest(time) =>
        nodeData.context.parent ! ChildLatencyResponse(self, time)
      case ChildLatencyResponse(childNode, requestTime) =>
        val childHost: Host = system.getHostByNode(childNode)
        if (testing) childNode match {
          case nodeData.childNode1 => childNode1Latency = Some(operator.host.lastLatencies(childHost).toDuration)
          case nodeData.childNode2 => childNode2Latency = Some(operator.host.lastLatencies(childHost).toDuration)
        } else childNode match {
          case nodeData.childNode1 => childNode1Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
          case nodeData.childNode2 => childNode2Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
        }
        if (childNode1Latency.isDefined && childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined && childNode2PathLatency.isDefined) {
          val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
          val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
          if (pathLatency1.compareTo(pathLatency2) >= 0) nodeData.context.parent ! PathLatency(self, pathLatency1)
          else nodeData.context.parent ! PathLatency(self, pathLatency2)
          if (logging && latencyRequirements.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after " +
              s"${math.max(pathLatency1.toMillis, pathLatency2.toMillis)} ms. (Calculated every $interval seconds.)")
          latencyRequirements.foreach(lr =>
            if (isRequirementNotMet(pathLatency1, lr)) query.addViolatedDemand(Violation(operator, lr)))
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
        if (childNode1Latency.isDefined && childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined && childNode2PathLatency.isDefined) {
          val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
          val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
          if (pathLatency1.compareTo(pathLatency2) >= 0) nodeData.context.parent ! PathLatency(self, pathLatency1)
          else nodeData.context.parent ! PathLatency(self, pathLatency2)
          if (logging && latencyRequirements.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after " +
              s"${math.max(pathLatency1.toMillis, pathLatency2.toMillis)} ms. (Calculated every $interval seconds.)")
          latencyRequirements.foreach(lr =>
            if (isRequirementNotMet(pathLatency1, lr)) query.addViolatedDemand(Violation(operator, lr)))
          childNode1Latency = None
          childNode2Latency = None
          childNode1PathLatency = None
          childNode2PathLatency = None
        }
    }
  }

}

case class PathLatencyMonitorFactory(interval: Int, logging: Boolean, testing: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = PathLatencyLeafNodeMonitor()
  override def createUnaryNodeMonitor: UnaryNodeMonitor = PathLatencyUnaryNodeMonitor(interval, logging, testing)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = PathLatencyBinaryNodeMonitor(interval, logging, testing)

}
