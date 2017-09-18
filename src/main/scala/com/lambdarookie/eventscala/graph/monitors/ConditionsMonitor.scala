package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef}
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.Utilities
import com.lambdarookie.eventscala.backend.system.traits.Host
import com.lambdarookie.eventscala.data.Events.Event

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

case class ConditionsMonitor(frequencyInterval: Int, proximityInterval: Int, logging: Boolean) extends Monitor {
  private var frequencyConditions: Option[Set[FrequencyCondition]] = None
  private var proximityConditions: Option[Set[ProximityCondition]] = None
  private var currentOutput: Option[Int] = None
  private var farthestDistance: Option[Int] = None
  private var lowestFrequency: Option[Ratio] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val demands: Set[Demand] = nodeData.query.demands
    if (frequencyConditions.isEmpty)
      frequencyConditions = Some(demands.flatMap(_.conditions.collect { case fc: FrequencyCondition => fc }))
    if (proximityConditions.isEmpty)
      proximityConditions = Some(demands.flatMap(_.conditions.collect { case pc: ProximityCondition => pc }))
    if (nodeData.isInstanceOf[LeafNodeData]) {
      currentOutput = Some(0)
      val parent: ActorRef = nodeData.context.parent
      if (frequencyInterval > 0) nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(frequencyInterval, TimeUnit.SECONDS),
        interval = FiniteDuration(frequencyInterval, TimeUnit.SECONDS),
        runnable = () => {
          parent ! Ratio(currentOutput.get.instances, frequencyInterval.sec)
          currentOutput = Some(0)
        })
      if (proximityInterval > 0) nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(proximityInterval, TimeUnit.SECONDS),
        runnable = () => parent ! nodeData.system.getHostByNode(nodeData.context.self).position)
    }
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val context: ActorContext = nodeData.context
    val parent: ActorRef = context.parent
    val self: ActorRef = context.self
    val host: Host = nodeData.system.getHostByNode(self)
    val demands: Set[Demand] = nodeData.query.demands
    if (frequencyConditions.isEmpty)
      frequencyConditions = Some(demands.flatMap(_.conditions.collect { case fc: FrequencyCondition => fc }))
    if (proximityConditions.isEmpty)
      proximityConditions = Some(demands.flatMap(_.conditions.collect { case pc: ProximityCondition => pc }))
    nodeData match {
      case _: LeafNodeData => if (logging && (message.isInstanceOf[Coordinate] || message.isInstanceOf[Ratio]))
        println("ERROR:\tA leaf node should not have gotten this message.")
      case _: UnaryNodeData => message match {
        case f: Ratio if frequencyConditions.isEmpty => parent ! f
        case f: Ratio if frequencyConditions.nonEmpty =>
          lowestFrequency = Some(f)
          if (logging) println(s"LOG:\t\tDescendant leaf node with the lowest frequency emits ${lowestFrequency.get}")
          setIfFrequencyConditionsFulfilled()
          parent ! f
        case c: Coordinate if proximityConditions.isEmpty => parent ! c
        case c: Coordinate if proximityConditions.nonEmpty =>
          farthestDistance = Some(Utilities.calculateDistance(host.position, c))
          if (logging)
            println(s"LOG:\t\tProximity from $host to the farthest leaf host is ${farthestDistance.get} meters.")
          setIfProximityConditionsFulfilled()
          parent ! c
        case _ => // We don't care about messages that aren't coordinates or frequency
      }
      case _: BinaryNodeData => message match {
        case f: Ratio if frequencyConditions.isEmpty => parent ! f
        case f: Ratio if frequencyConditions.nonEmpty =>
          if (lowestFrequency.isEmpty) {
            lowestFrequency = Some(f)
          } else {
            lowestFrequency = Some(min(f, lowestFrequency.get))
            if (logging) println(s"LOG:\t\tDescendant leaf node with the lowest frequency emits ${lowestFrequency.get}")
            setIfFrequencyConditionsFulfilled()
          }
          parent ! f
        case c: Coordinate if proximityConditions.isEmpty => parent ! c
        case c: Coordinate if proximityConditions.nonEmpty =>
          if (farthestDistance.isEmpty) {
            farthestDistance = Some(Utilities.calculateDistance(host.position, c))
          } else {
            farthestDistance = Some(math.max(farthestDistance.get, Utilities.calculateDistance(host.position, c)))
            if (logging)
              println(s"LOG:\t\tProximity from $host to the farthest leaf host is ${farthestDistance.get} meters.")
            setIfProximityConditionsFulfilled()
          }
          parent ! c
        case _ => // We don't care about messages that aren't coordinates or frequency
      }
    }
  }

  override def onEventEmit(event: Event, nodeData: NodeData): Unit =
    if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)

  override def copy: ConditionsMonitor = ConditionsMonitor(frequencyInterval, proximityInterval, logging)

  private def setIfFrequencyConditionsFulfilled(): Unit = frequencyConditions.get.foreach{ fc =>
    require(fc.ratio.timeSpan.toSeconds <= frequencyInterval)
    val current: Ratio = lowestFrequency.get
    val expected: Ratio = fc.ratio
    fc.booleanOperator match {
      case Equal =>         fc.notFulfilled = !(current == expected)
      case NotEqual =>      fc.notFulfilled = !(current != expected)
      case Greater =>       fc.notFulfilled = !(current > expected)
      case GreaterEqual =>  fc.notFulfilled = !(current >= expected)
      case Smaller =>       fc.notFulfilled = !(current < expected)
      case SmallerEqual =>  fc.notFulfilled = !(current <= expected)
    }
    lowestFrequency = None
  }

  private def setIfProximityConditionsFulfilled(): Unit = proximityConditions.get.foreach{ pc =>
    val current: Int = farthestDistance.get
    val expected: Int = pc.distance.toMeter
    pc.booleanOperator match {
      case Equal =>         pc.notFulfilled = !(current == expected)
      case NotEqual =>      pc.notFulfilled = !(current != expected)
      case Greater =>       pc.notFulfilled = !(current > expected)
      case GreaterEqual =>  pc.notFulfilled = !(current >= expected)
      case Smaller =>       pc.notFulfilled = !(current < expected)
      case SmallerEqual =>  pc.notFulfilled = !(current <= expected)
    }
    farthestDistance = None
  }
}

