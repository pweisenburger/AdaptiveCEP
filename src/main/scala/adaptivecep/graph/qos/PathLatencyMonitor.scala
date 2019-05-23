package adaptivecep.graph.qos

import java.time._
import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events.{CostReport, HostPropsResponse, LatencyResponse}

import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{ActorRef, Cancellable}

import scala.concurrent.duration.FiniteDuration
import adaptivecep.data.Queries._

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, duration: Duration)

trait PathLatencyMonitor {

  var costs: Map[ActorRef, Cost] = Map.empty[ActorRef, Cost] withDefaultValue(Cost(scala.concurrent.duration.FiniteDuration(0, TimeUnit.SECONDS), 100))

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

  var delay: Boolean = true

  override def onCreated(nodeData: LeafNodeData): Unit = {
    if (nodeData.requirements.collect{ case lr: LatencyRequirement => lr }.nonEmpty)
      println("PROBLEM:\tLatency requirements for leaf nodes are ignored, as leaf node latency is always considered 0.")
  }

  override def onMessageReceive(message: Any, nodeData: LeafNodeData): Unit = {
    message match {
      case ChildLatencyRequest(time) =>
        nodeData.context.system.scheduler.scheduleOnce(
          FiniteDuration(costs(nodeData.parent).duration.toMillis * 2, TimeUnit.MILLISECONDS),
          () => {
            nodeData.parent ! ChildLatencyResponse(nodeData.context.self, time)
            nodeData.parent ! PathLatency(nodeData.context.self, Duration.ZERO)
          })
      case CostReport(costMap) => costs = costMap
      case _ =>
    }
  }
}

case class PathLatencyUnaryNodeMonitor(interval: Int, logging: Boolean)
  extends PathLatencyMonitor with UnaryNodeMonitor {

  val clock: Clock = Clock.systemDefaultZone
  var scheduledTask: Cancellable = null
  var violatedRequirements: Set[Requirement] = Set.empty[Requirement]
  //var delay: Boolean = true
  var latency: Option[Duration] = None
  var childNode: ActorRef = null
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None


  override def onCreated(nodeData: UnaryNodeData): Unit = {
    childNode = nodeData.childNode
    if(scheduledTask == null) {
      scheduledTask = nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
        runnable = () => childNode ! ChildLatencyRequest(clock.instant))
    }
  }

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = {
    val callbackNodeData: NodeData = NodeData(nodeData.name, nodeData.requirements, nodeData.context)
    val latencyRequirements: Set[LatencyRequirement] =
      nodeData.requirements.collect { case lr: LatencyRequirement => lr }
    message match {
      case CostReport(costMap) => costs = costMap
      case ChildLatencyRequest(time) =>
        nodeData.context.system.scheduler.scheduleOnce(
          FiniteDuration(costs(nodeData.parent).duration.toMillis * 2, TimeUnit.MILLISECONDS),
          () => {nodeData.parent ! ChildLatencyResponse(nodeData.context.self, time)})
      case ChildLatencyResponse(_, requestTime) =>
        childNodeLatency = Some((Duration.between(requestTime, clock.instant)).dividedBy(2))//Some(Duration.between(requestTime, clock.instant))
        if (childNodePathLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          latency = Some(pathLatency)
          nodeData.parent ! PathLatency(nodeData.context.self, pathLatency)
          if (logging && latencyRequirements.nonEmpty)
            println(
              s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. " +
              s"(Calculated every $interval seconds.)")
          violatedRequirements = Set.empty[Requirement]
          latencyRequirements.foreach(
            lr =>{
              if (isRequirementNotMet(pathLatency, lr)) {
                lr.callback(callbackNodeData)
                violatedRequirements += lr
              }
            })
          childNodeLatency = None
          childNodePathLatency = None
        }
      case PathLatency(_, duration) =>
        childNodePathLatency = Some(duration)
        if (childNodeLatency.isDefined) {
          val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
          latency = Some(pathLatency)
          nodeData.parent ! PathLatency(nodeData.context.self, pathLatency)
          if (logging && latencyRequirements.nonEmpty)
            println(
              s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. " +
              s"(Calculated every $interval seconds.)")
          violatedRequirements = Set.empty[Requirement]
          latencyRequirements.foreach(
            lr =>
              if (isRequirementNotMet(pathLatency, lr)){
                lr.callback(callbackNodeData)
                violatedRequirements += lr
              }
            )
          childNodeLatency = None
          childNodePathLatency = None
        }
      case _ =>
    }
  }

}

case class PathLatencyBinaryNodeMonitor(interval: Int, logging: Boolean)
  extends PathLatencyMonitor with BinaryNodeMonitor {

  var violatedRequirements: Set[Requirement] = Set.empty[Requirement]
  //var delay: Boolean = true
  var latency: Option[Duration] = None
  var scheduledTask: Cancellable = null
  val clock: Clock = Clock.systemDefaultZone
  var childNode1: ActorRef = _
  var childNode2: ActorRef = _
  var childNode1Latency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  override def onCreated(nodeData: BinaryNodeData): Unit = {
    childNode1 = nodeData.childNode1
    childNode2 = nodeData.childNode2
    if(scheduledTask == null) {
      scheduledTask = nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
        runnable = () => {
          childNode1 ! ChildLatencyRequest(clock.instant)
          childNode2 ! ChildLatencyRequest(clock.instant)
        })
    }
  }

  override def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = {
    val callbackNodeData: NodeData = NodeData(nodeData.name, nodeData.requirements, nodeData.context)
    val latencyRequirements: Set[LatencyRequirement] =
      nodeData.requirements.collect { case lr: LatencyRequirement => lr }
    message match {
      case CostReport(costMap) => costs = costMap
      case ChildLatencyRequest(time) =>
        nodeData.context.system.scheduler.scheduleOnce(
          FiniteDuration(costs(nodeData.parent).duration.toMillis * 2, TimeUnit.MILLISECONDS),
          () => {nodeData.parent ! ChildLatencyResponse(nodeData.context.self, time)})



       // nodeData.parent ! ChildLatencyResponse(nodeData.context.self, time.minusMillis(costs(nodeData.parent)._1.toMillis * 2))
      /*if(delay) {
          nodeData.parent ! ChildLatencyResponse(nodeData.context.self, time.minusMillis(40))
        } else {
          nodeData.parent ! ChildLatencyResponse(nodeData.context.self, time)
        }*/
      case ChildLatencyResponse(childNode, requestTime) =>
        childNode match {
          case nodeData.childNode1 =>
            childNode1Latency = Some((Duration.between(requestTime, clock.instant)).dividedBy(2))
          case nodeData.childNode2 =>
            childNode2Latency = Some((Duration.between(requestTime, clock.instant)).dividedBy(2))
          case _ =>
        }
        if (childNode1Latency.isDefined &&
          childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined &&
          childNode2PathLatency.isDefined) {
          val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
          val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
          if (pathLatency1.compareTo(pathLatency2) >= 0) {
            latency = Some(pathLatency1)
            nodeData.parent ! PathLatency(nodeData.context.self, pathLatency1)
            if (logging && latencyRequirements.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. " +
                s"(Calculated every $interval seconds.)")
            violatedRequirements = Set.empty[Requirement]
            latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency1, lr)){
              lr.callback(callbackNodeData)
              violatedRequirements += lr
            })
          } else {
            latency = Some(pathLatency2)
            nodeData.parent ! PathLatency(nodeData.context.self, pathLatency2)
            if (logging && latencyRequirements.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. " +
                s"(Calculated every $interval seconds.)")
            violatedRequirements = Set.empty[Requirement]
            latencyRequirements.foreach(lr => if (isRequirementNotMet(pathLatency2, lr)){
              lr.callback(callbackNodeData)
              violatedRequirements += lr
            })
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
          case _ =>
        }
        if (childNode1Latency.isDefined &&
          childNode2Latency.isDefined &&
          childNode1PathLatency.isDefined &&
          childNode2PathLatency.isDefined) {
          val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
          val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
          if (pathLatency1.compareTo(pathLatency2) >= 0) {
            latency = Some(pathLatency1)
            nodeData.parent ! PathLatency(nodeData.context.self, pathLatency1)
            if (logging && latencyRequirements.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. " +
                s"(Calculated every $interval seconds.)")
            violatedRequirements = Set.empty[Requirement]
            latencyRequirements.foreach(lr =>
              if (isRequirementNotMet(pathLatency1, lr)){
                lr.callback(callbackNodeData)
                violatedRequirements += lr
              }
            )
          } else {
            latency = Some(pathLatency2)
            nodeData.parent ! PathLatency(nodeData.context.self, pathLatency2)
            if (logging && nodeData.requirements.collect { case lr: LatencyRequirement => lr }.nonEmpty)
              println(
                s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. " +
                s"(Calculated every $interval seconds.)")
            violatedRequirements = Set.empty[Requirement]
            nodeData.requirements.collect { case lr: LatencyRequirement => lr }.foreach(lr =>
              if (isRequirementNotMet(pathLatency2, lr)){
                lr.callback(callbackNodeData)
                violatedRequirements += lr
              }
            )
          }
          childNode1Latency = None
          childNode2Latency = None
          childNode1PathLatency = None
          childNode2PathLatency = None
        }
      case _ =>
    }

  }

}

case class PathLatencyMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = PathLatencyLeafNodeMonitor()
  override def createUnaryNodeMonitor: UnaryNodeMonitor = PathLatencyUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = PathLatencyBinaryNodeMonitor(interval, logging)

}
