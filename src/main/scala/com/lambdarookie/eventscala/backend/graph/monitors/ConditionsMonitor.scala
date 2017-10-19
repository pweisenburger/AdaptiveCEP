package com.lambdarookie.eventscala.backend.graph.monitors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef}
import com.lambdarookie.eventscala.backend.qos.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.Host
import com.lambdarookie.eventscala.data.Events.Event

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

case class CoordinateWrapper(coordinates: Set[Coordinate])

case class ConditionsMonitor(frequencyInterval: Int, proximityInterval: Int, logging: Boolean) extends Monitor {
  private var frequencyConditions: Option[Set[FrequencyCondition]] = None
  private var proximityConditions: Option[Set[ProximityCondition]] = None
  private var currentOutput: Option[Int] = None
  private var leafCoordinates: Option[Set[Coordinate]] = None
  private var lowestFrequency: Option[Ratio] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val demands: Set[Demand] = nodeData.query.demands
    if (frequencyConditions.isEmpty && demands.exists(_.conditions.exists(_.isInstanceOf[FrequencyCondition])))
      frequencyConditions = Some(demands.flatMap(_.conditions.collect { case fc: FrequencyCondition => fc }))
    if (proximityConditions.isEmpty && demands.exists(_.conditions.exists(_.isInstanceOf[ProximityCondition])))
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
        runnable = () => parent ! CoordinateWrapper(Set(nodeData.system.getHostByNode(nodeData.context.self).position)))
    }
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val context: ActorContext = nodeData.context
    val parent: ActorRef = context.parent
    val self: ActorRef = context.self
    val host: Host = nodeData.system.getHostByNode(self)
    val demands: Set[Demand] = nodeData.query.demands
    if (frequencyConditions.isEmpty && demands.exists(_.conditions.exists(_.isInstanceOf[FrequencyCondition])))
      frequencyConditions = Some(demands.flatMap(_.conditions.collect { case fc: FrequencyCondition => fc }))
    if (proximityConditions.isEmpty && demands.exists(_.conditions.exists(_.isInstanceOf[ProximityCondition])))
      proximityConditions = Some(demands.flatMap(_.conditions.collect { case pc: ProximityCondition => pc }))
    nodeData match {
      case _: LeafNodeData => if (logging && (message.isInstanceOf[CoordinateWrapper] || message.isInstanceOf[Ratio]))
        println("ERROR:\tA leaf node should not have gotten this message.")
      case _: UnaryNodeData => message match {
        case f: Ratio if frequencyConditions.isEmpty => parent ! f
        case f: Ratio if frequencyConditions.nonEmpty =>
          lowestFrequency = Some(f)
          if (logging) println(s"LOG:\t\tDescendant leaf node with the lowest frequency emits ${lowestFrequency.get}")
          setIfFrequencyConditionsFulfilled()
          parent ! f
        case cw@CoordinateWrapper(_) if proximityConditions.isEmpty => parent ! cw
        case cw@CoordinateWrapper(cs) if proximityConditions.nonEmpty =>
          val farthestDistance: Distance = cs.foldLeft(0.m)((d, c) => max(d, calculateDistance(host.position, c)))
          if (logging)
            println(s"LOG:\t\tProximity from $host to the farthest leaf host is $farthestDistance.")
          setIfProximityConditionsFulfilled(farthestDistance)
          parent ! cw
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
        case CoordinateWrapper(cs) if proximityConditions.isEmpty =>
          if (leafCoordinates.isEmpty)
            leafCoordinates = Some(cs)
          else
            parent ! CoordinateWrapper(leafCoordinates.get ++ cs)
        case CoordinateWrapper(cs) if proximityConditions.nonEmpty =>
          if (leafCoordinates.isEmpty) {
            leafCoordinates = Some(cs)
          } else {
            val leafCoordinates: Set[Coordinate] = this.leafCoordinates.get ++ cs
            val farthestDistance: Distance =
              leafCoordinates.foldLeft(0.m)((d, c) => max(d, calculateDistance(host.position, c)))
            if (logging)
              println(s"LOG:\t\tProximity from $host to the farthest leaf host is $farthestDistance")
            setIfProximityConditionsFulfilled(farthestDistance)
            parent ! CoordinateWrapper(leafCoordinates)
          }
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
      case Equal =>         fc.asInstanceOf[ConditionImpl].notFulfilled = !(current == expected)
      case NotEqual =>      fc.asInstanceOf[ConditionImpl].notFulfilled = !(current != expected)
      case Greater =>       fc.asInstanceOf[ConditionImpl].notFulfilled = !(current > expected)
      case GreaterEqual =>  fc.asInstanceOf[ConditionImpl].notFulfilled = !(current >= expected)
      case Smaller =>       fc.asInstanceOf[ConditionImpl].notFulfilled = !(current < expected)
      case SmallerEqual =>  fc.asInstanceOf[ConditionImpl].notFulfilled = !(current <= expected)
    }
    lowestFrequency = None
  }

  private def setIfProximityConditionsFulfilled(farthestDistance: Distance): Unit = proximityConditions.get.foreach{ pc =>
    val expected: Distance = pc.distance
    pc.booleanOperator match {
      case Equal =>         pc.asInstanceOf[ConditionImpl].notFulfilled = !(farthestDistance == expected)
      case NotEqual =>      pc.asInstanceOf[ConditionImpl].notFulfilled = !(farthestDistance != expected)
      case Greater =>       pc.asInstanceOf[ConditionImpl].notFulfilled = !(farthestDistance > expected)
      case GreaterEqual =>  pc.asInstanceOf[ConditionImpl].notFulfilled = !(farthestDistance >= expected)
      case Smaller =>       pc.asInstanceOf[ConditionImpl].notFulfilled = !(farthestDistance < expected)
      case SmallerEqual =>  pc.asInstanceOf[ConditionImpl].notFulfilled = !(farthestDistance <= expected)
    }
    leafCoordinates = None
  }

  /**
    * Calculate the distance from a coordinate to another coordinate
    * @param from Coordinate of type [[Coordinate]]
    * @param to Coordinate of type [[Coordinate]]
    * @return Distance in meters
    */
  private def calculateDistance(from: Coordinate, to: Coordinate): Distance = {
    import math._

    val R = 6371 // Radius of the earth
    val latDistance = toRadians(from.latitude - to.latitude)
    val lonDistance = toRadians(from.longitude - to.longitude)
    val a = sin(latDistance / 2) * sin(latDistance / 2) +
      cos(toRadians(from.latitude)) * cos(toRadians(to.latitude))* sin(lonDistance / 2) * sin(lonDistance / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1-a))
    val distance = R * c * 1000 // convert to meters
    val height = from.altitude - to.altitude
    sqrt(pow(distance, 2) + pow(height, 2)).toInt.m
  }
}

