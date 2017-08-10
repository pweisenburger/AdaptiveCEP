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

  val logging: Boolean

  def isDemandNotMet(latency: Duration, ld: LatencyDemand): Boolean = {
    val met: Boolean = ld.booleanOperator match {
      case Equal =>        latency.compareTo(ld.timeSpan.toDuration) == 0
      case NotEqual =>     latency.compareTo(ld.timeSpan.toDuration) != 0
      case Greater =>      latency.compareTo(ld.timeSpan.toDuration) >  0
      case GreaterEqual => latency.compareTo(ld.timeSpan.toDuration) >= 0
      case Smaller =>      latency.compareTo(ld.timeSpan.toDuration) <  0
      case SmallerEqual => latency.compareTo(ld.timeSpan.toDuration) <= 0
    }
    !met
  }

  def areConditionsMet(ld: LatencyDemand): Boolean = if(ld.conditions.exists(_.notFulfilled)) {
    if(logging) println("LOG:\tSome conditions for the latency demand are not met.")
    false
  } else true

}

case class PathLatencyLeafNodeMonitor(logging: Boolean) extends PathLatencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = {
    if (nodeData.query.demands.collect{ case ld: LatencyDemand => ld }.nonEmpty && logging)
      println("LOG:\tLatency demands for leaf nodes are ignored, as leaf node latency is always considered 0.")
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
      nodeData.system.measureLowestLatencies()
      nodeData.childNode ! ChildLatencyRequest(clock.instant)
    })

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = {
    val query: Query = nodeData.query
    val latencyDemands: Set[LatencyDemand] =
      query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
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
          if (logging && latencyDemands.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathLatency.toMillis} ms. " +
              s"(Calculated every $interval seconds.)")
          latencyDemands.foreach(ld =>
            if (isDemandNotMet(pathLatency, ld)) query.addViolatedDemand(Violation(operator, ld)))
          childNodeLatency = None
          childNodePathLatency = None
        }
      case PathLatency(_, duration) =>
        childNodePathLatency = Some(duration)
        if (childNodeLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          nodeData.context.parent ! PathLatency(self, pathLatency)
          if (logging && latencyDemands.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathLatency.toMillis} ms. " +
              s"(Calculated every $interval seconds.)")
          latencyDemands.foreach(ld =>
            if (isDemandNotMet(pathLatency, ld)) query.addViolatedDemand(Violation(operator, ld)))
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
    initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      nodeData.system.measureLowestLatencies()
      nodeData.childNode1 ! ChildLatencyRequest(clock.instant)
      nodeData.childNode2 ! ChildLatencyRequest(clock.instant)
    })

  override def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = {
    val query = nodeData.query
    val latencyDemands: Set[LatencyDemand] =
      query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
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
          if (logging && latencyDemands.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after " +
              s"${math.max(pathLatency1.toMillis, pathLatency2.toMillis)} ms. (Calculated every $interval seconds.)")
          latencyDemands.foreach(ld =>
            if (isDemandNotMet(pathLatency1, ld)) query.addViolatedDemand(Violation(operator, ld)))
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
          if (logging && latencyDemands.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after " +
              s"${math.max(pathLatency1.toMillis, pathLatency2.toMillis)} ms. (Calculated every $interval seconds.)")
          latencyDemands.foreach(ld =>
            if (isDemandNotMet(pathLatency1, ld)) query.addViolatedDemand(Violation(operator, ld)))
          childNode1Latency = None
          childNode2Latency = None
          childNode1PathLatency = None
          childNode2PathLatency = None
        }
    }
  }

}

case class PathLatencyMonitorFactory(interval: Int, logging: Boolean, testing: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = PathLatencyLeafNodeMonitor(logging)
  override def createUnaryNodeMonitor: UnaryNodeMonitor = PathLatencyUnaryNodeMonitor(interval, logging, testing)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = PathLatencyBinaryNodeMonitor(interval, logging, testing)

}
