package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef}
import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.Host

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

case class ConditionsMonitor(interval: Int, logging: Boolean) extends Monitor {
  var highestProximity: Option[Int] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val self = nodeData.context.self
    val host: Host = nodeData.system.getHostByNode(self)
    val frequencyConditions: Set[FrequencyCondition] =
      nodeData.query.demands.flatMap(_.conditions.collect { case fc: FrequencyCondition => fc })
    if (nodeData.isInstanceOf[LeafNodeData]) nodeData.context.parent ! host.position
    if (frequencyConditions.nonEmpty) {
      nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          frequencyConditions.foreach {
            fc => {
              require(fc.ratio.timeSpan.toSeconds <= interval)
              val current: Int = host.measureFrequency().instances.getInstanceNum
              if (logging) println(s"LOG:\t\tOn average, node `${nodeData.name}` emits $current events every " +
                s"${fc.ratio.timeSpan.toSeconds} seconds. (Calculated every $interval seconds.)")
              val expected: Int = fc.ratio.instances.getInstanceNum
              fc.booleanOperator match {
                case Equal =>         fc.notFulfilled = !(current == expected)
                case NotEqual =>      fc.notFulfilled = !(current != expected)
                case Greater =>       fc.notFulfilled = !(current > expected)
                case GreaterEqual =>  fc.notFulfilled = !(current >= expected)
                case Smaller =>       fc.notFulfilled = !(current < expected)
                case SmallerEqual =>  fc.notFulfilled = !(current <= expected)
              }
            }
          }
        }
      )
    }
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val proximityConditions: Set[ProximityCondition] =
      nodeData.query.demands.flatMap(_.conditions.collect { case pc: ProximityCondition => pc })
    val context: ActorContext = nodeData.context
    val parent: ActorRef = context.parent
    val self: ActorRef = context.self
    val host: Host = nodeData.system.getHostByNode(self)
    nodeData match {
      case _: LeafNodeData => if (logging && message.isInstanceOf[Coordinate])
        println("ERROR:\tA leaf node should not have gotten this message.")
      case _: UnaryNodeData => message match {
        case c: Coordinate if proximityConditions.isEmpty => parent ! c
        case c: Coordinate if proximityConditions.nonEmpty =>
          highestProximity = Some(host.position.calculateDistanceTo(c))
          if (logging)
            println(s"LOG:\t\tProximity from $host to the farthest leaf host is ${highestProximity.get} meters.")
          setIfConditionsFulfilled(proximityConditions)
          parent ! c
        case _ => // We don't care about messages that aren't coordinates
      }
      case _: BinaryNodeData => message match {
        case c: Coordinate if proximityConditions.isEmpty => parent ! c
        case c: Coordinate if proximityConditions.nonEmpty =>
          if (highestProximity.isEmpty) {
            highestProximity = Some(host.position.calculateDistanceTo(c))
          } else {
            highestProximity = Some(math.max(highestProximity.get, host.position.calculateDistanceTo(c)))
            if (logging)
              println(s"LOG:\t\tProximity from $host to the farthest leaf host is ${highestProximity.get} meters.")
            setIfConditionsFulfilled(proximityConditions)
          }
          parent ! c
        case _ => // We don't care about messages that aren't coordinates
      }
    }
  }

  override def copy: ConditionsMonitor = ConditionsMonitor(interval, logging)

  private def setIfConditionsFulfilled(proximityConditions: Set[ProximityCondition]): Unit = {
    proximityConditions.foreach { pc => {
      val current: Int = highestProximity.get
      val expected: Int = pc.distance.toMeter
      pc.booleanOperator match {
        case Equal =>         pc.notFulfilled = !(current == expected)
        case NotEqual =>      pc.notFulfilled = !(current != expected)
        case Greater =>       pc.notFulfilled = !(current > expected)
        case GreaterEqual =>  pc.notFulfilled = !(current >= expected)
        case Smaller =>       pc.notFulfilled = !(current < expected)
        case SmallerEqual =>  pc.notFulfilled = !(current <= expected)
      }
    }}
    highestProximity = None
  }
}
