package adaptivecep.graph.qos

import java.time.{Clock, Duration, Instant}
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import adaptivecep.data.Queries._

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, duration: Duration)

// TODO Collecting the requirements is done too often -- remove code duplicates.
// TODO Pass `nodeData` to callbacks.

trait PathLatencyMonitor {

  def isRequirementNotMet(latency: Duration, lr: LatencyRequirement): Boolean = {
    val met: Boolean = lr.operator match {
      case Equal =>        latency.compareTo(lr.duration) == 0
      case NotEqual =>     latency.compareTo(lr.duration) != 0
      case Greater =>      latency.compareTo(lr.duration) >  0
      case GreaterEqual => latency.compareTo(lr.duration) >= 0
      case Smaller =>      latency.compareTo(lr.duration) <  0
      case SmallerEqual => latency.compareTo(lr.duration) <= 0
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

case class PathLatencyUnaryNodeMonitor(interval: Int, logging: Boolean) extends PathLatencyMonitor with UnaryNodeMonitor {

  val clock: Clock = Clock.systemDefaultZone
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None

  override def onCreated(nodeData: UnaryNodeData): Unit = nodeData.context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => nodeData.childNode ! ChildLatencyRequest(clock.instant))

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = message match {
    case ChildLatencyRequest(time) =>
      nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
    case ChildLatencyResponse(_, requestTime) =>
      childNodeLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (childNodePathLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
        if (logging && nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
          println(s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. (Calculated every $interval seconds.)")
        nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.foreach(lr =>
          if (isRequirementNotMet(pathLatency, lr)) lr.callback(nodeData.name))
        childNodeLatency = None
        childNodePathLatency = None
      }
    case PathLatency(_, duration) =>
      childNodePathLatency = Some(duration)
      if (childNodeLatency.isDefined) {
        val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
        nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
        if (logging && nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
          println(s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. (Calculated every $interval seconds.)")
        nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.foreach(lr =>
          if (isRequirementNotMet(pathLatency, lr)) lr.callback(nodeData.name))
        childNodeLatency = None
        childNodePathLatency = None
      }
  }

}

case class PathLatencyBinaryNodeMonitor(interval: Int, logging: Boolean) extends PathLatencyMonitor with BinaryNodeMonitor {

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
          if (logging && nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. (Calculated every $interval seconds.)")
          nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.foreach(lr =>
            if (isRequirementNotMet(pathLatency1, lr)) lr.callback(nodeData.name))
        } else {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
          if (logging && nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. (Calculated every $interval seconds.)")
          nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.foreach(lr =>
            if (isRequirementNotMet(pathLatency2, lr)) lr.callback(nodeData.name))
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
          if (logging && nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. (Calculated every $interval seconds.)")
          nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.foreach(lr =>
            if (isRequirementNotMet(pathLatency1, lr)) lr.callback(nodeData.name))
        } else {
          nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
          if (logging && nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
            println(s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. (Calculated every $interval seconds.)")
          nodeData.query.requirements.collect{ case lr: LatencyRequirement => lr }.foreach(lr =>
            if (isRequirementNotMet(pathLatency2, lr)) lr.callback(nodeData.name))
        }
        childNode1Latency = None
        childNode2Latency = None
        childNode1PathLatency = None
        childNode2PathLatency = None
      }
  }

}

case class PathLatencyMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = PathLatencyLeafNodeMonitor()
  override def createUnaryNodeMonitor: UnaryNodeMonitor = PathLatencyUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = PathLatencyBinaryNodeMonitor(interval, logging)

}
