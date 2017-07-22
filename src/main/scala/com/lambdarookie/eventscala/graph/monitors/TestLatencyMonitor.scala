package com.lambdarookie.eventscala.graph.monitors

import java.time._
import java.util.concurrent.TimeUnit

import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.TestSystem

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import com.lambdarookie.eventscala.data.Queries._

trait TestLatencyMonitor {
  val testSystem: TestSystem

  def isRequirementNotMet(latency: Duration, lr: LatencyRequirement): Boolean = {
    val met: Boolean = lr.boolOperator match {
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

case class TestLatencyLeafNodeMonitor(testSystem: TestSystem) extends TestLatencyMonitor with LeafNodeMonitor {

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

case class TestLatencyUnaryNodeMonitor(interval: Int, logging: Boolean, testSystem: TestSystem)
  extends TestLatencyMonitor with UnaryNodeMonitor {

  val clock: Clock = Clock.systemDefaultZone
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None

  override def onCreated(nodeData: UnaryNodeData): Unit = nodeData.context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => nodeData.childNode ! ChildLatencyRequest(clock.instant))

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = {
    val callbackNodeData: NodeData = NodeData(nodeData.name, nodeData.query, nodeData.context)
    val latencyRequirements: Set[LatencyRequirement] =
      nodeData.query.requirements.collect { case lr: LatencyRequirement => lr }
    message match {
      // No need for measuring since this is a test monitor
      case ChildLatencyRequest(time) => nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
      case ChildLatencyResponse(childNode, _) =>
        childNodeLatency = Some(testSystem.getHostByNode(nodeData.context.self)
          .get.lastLatencies(testSystem.getHostByNode(childNode).get).latency.toDuration)
        if (childNodePathLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
          if (logging && latencyRequirements.nonEmpty)
            println(
              s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. " +
                s"(Calculated every $interval seconds.)")
          //TODO: Handle violated demand
          //          latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency, lr)) lr.callback(callbackNodeData))
          childNodeLatency = None
          childNodePathLatency = None
        }
      case PathLatency(_, duration) =>
        childNodePathLatency = Some(duration)
        if (childNodeLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
          if (logging && latencyRequirements.nonEmpty)
            println(
              s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. " +
                s"(Calculated every $interval seconds.)")
          //TODO: Handle violated demand
          //          latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency, lr)) lr.callback(callbackNodeData))
          childNodeLatency = None
          childNodePathLatency = None
        }
    }
  }

}

case class TestLatencyBinaryNodeMonitor(interval: Int, logging: Boolean, testSystem: TestSystem)
  extends TestLatencyMonitor with BinaryNodeMonitor {

  val clock: Clock = Clock.systemDefaultZone
  var childNode1Latency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  override def onCreated(nodeData: BinaryNodeData): Unit = nodeData.context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      nodeData.childNode1 ! ChildLatencyRequest(clock.instant)
      nodeData.childNode2 ! ChildLatencyRequest(clock.instant)
    })

  override def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = {
    val callbackNodeData: NodeData = NodeData(nodeData.name, nodeData.query, nodeData.context)
    val latencyRequirements: Set[LatencyRequirement] =
      nodeData.query.requirements.collect { case lr: LatencyRequirement => lr }
    message match {
      case ChildLatencyRequest(time) =>
        nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
      case ChildLatencyResponse(childNode, _) =>
        childNode match {
          case nodeData.childNode1 => childNode1Latency = Some(testSystem.getHostByNode(nodeData.context.self)
            .get.lastLatencies(testSystem.getHostByNode(childNode).get).latency.toDuration)
          case nodeData.childNode2 => childNode2Latency = Some(testSystem.getHostByNode(nodeData.context.self)
            .get.lastLatencies(testSystem.getHostByNode(childNode).get).latency.toDuration)
        }
        if (childNode1Latency.isDefined &&
          childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined &&
          childNode2PathLatency.isDefined) {
          val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
          val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
          if (pathLatency1.compareTo(pathLatency2) >= 0) {
            nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency1)
            if (logging && latencyRequirements.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. " +
                  s"(Calculated every $interval seconds.)")
            //TODO: Handle violated demand
            //            latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency1, lr)) lr.callback(callbackNodeData))
          } else {
            nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
            if (logging && latencyRequirements.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. " +
                  s"(Calculated every $interval seconds.)")
            //TODO: Handle violated demand
            //            latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency2, lr)) lr.callback(callbackNodeData))
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
            if (logging && latencyRequirements.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. " +
                  s"(Calculated every $interval seconds.)")
            //TODO: Handle violated demand
            //            latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency1, lr)) lr.callback(callbackNodeData))
          } else {
            nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
            if (logging && nodeData.query.requirements.collect { case lr: LatencyRequirement => lr }.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. " +
                  s"(Calculated every $interval seconds.)")
            //TODO: Handle violated demand
            //            nodeData.query.requirements.collect { case lr: LatencyRequirement => lr }.foreach(lr =>
            //              if (isRequirementNotMet(pathLatency2, lr)) lr.callback(callbackNodeData))
          }
          childNode1Latency = None
          childNode2Latency = None
          childNode1PathLatency = None
          childNode2PathLatency = None
        }
    }
  }

}

case class TestLatencyMonitorFactory(interval: Int, logging: Boolean, testSystem: TestSystem) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = TestLatencyLeafNodeMonitor(testSystem)
  override def createUnaryNodeMonitor: UnaryNodeMonitor = TestLatencyUnaryNodeMonitor(interval, logging, testSystem)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = TestLatencyBinaryNodeMonitor(interval, logging, testSystem)

}
