package com.lambdarookie.eventscala.graph.monitors

import java.time._
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.qos.QoSUnits.TimeSpan
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._

trait LatencyMessage
case class ChildLatencyRequest(time: Instant) extends LatencyMessage
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant) extends LatencyMessage
case class PathLatency(childNode: ActorRef, duration: Duration) extends LatencyMessage

case class PathLatencyMonitor(interval: Int, logging: Boolean) extends Monitor {

  val clock: Clock = Clock.systemDefaultZone
  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None
  var childNode1Latency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val childNodes: Seq[ActorRef] = nodeData match {
      case lnd: LeafNodeData =>
        if (lnd.query.demands.nonEmpty && logging)
          println("PROBLEM:\tLatency demands for leaf nodes are ignored, as leaf node latency is always considered 0.")
        Seq.empty
      case und: UnaryNodeData => Seq(und.childNode)
      case bnd: BinaryNodeData => Seq(bnd.childNode1, bnd.childNode2)
    }
    if (childNodes.nonEmpty) nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        childNodes.foreach(_ ! ChildLatencyRequest(clock.instant()))
      })
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val system: System = nodeData.system
    val operator: Operator = system.nodesToOperators.now.apply(nodeData.context.self)

    def updateViolations(pathLatency: Duration, latencyDemands: Set[LatencyDemand]): Unit = {
      val violatedDemands: Set[LatencyDemand] = latencyDemands.filter(isDemandNotMet(pathLatency, _))
      nodeData.query.violations.now.foreach{
        case v@Violation(_, ld: LatencyDemand) if (latencyDemands -- violatedDemands).contains(ld) =>
          nodeData.query.removeViolation(v)
        case _ =>
      }
      val violations: Set[Violation] = violatedDemands.map(Violation(operator, _))
      if (violations.nonEmpty) system.fireDemandsViolated(violations)
    }

    if (message.isInstanceOf[LatencyMessage]) nodeData match {
      case _: LeafNodeData => message match {
        case ChildLatencyRequest(requestTime) =>
          nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, requestTime)
          nodeData.context.parent ! PathLatency(nodeData.context.self, Duration.ZERO)
      }
      case _: UnaryNodeData =>
        val latencyDemands: Set[LatencyDemand] =
          nodeData.query.demands.collect { case ld: LatencyDemand => ld }
        message match {
          case ChildLatencyRequest(time) =>
            nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
          case ChildLatencyResponse(_, requestTime) =>
            childNodeLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
            if (childNodePathLatency.isDefined) {
              val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
              nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
              if (logging && latencyDemands.nonEmpty)
                println(
                  s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. " +
                    s"(Calculated every $interval seconds.)")
              updateViolations(pathLatency, latencyDemands)
              childNodeLatency = None
              childNodePathLatency = None
            }
          case PathLatency(_, duration) =>
            childNodePathLatency = Some(duration)
            if (childNodeLatency.isDefined) {
              val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
              nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency)
              if (logging && latencyDemands.nonEmpty)
                println(
                  s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency. " +
                    s"(Calculated every $interval seconds.)")
              updateViolations(pathLatency, latencyDemands)
              childNodeLatency = None
              childNodePathLatency = None
            }
        }
      case bnd: BinaryNodeData =>
        val latencyDemands: Set[LatencyDemand] =
          nodeData.query.demands.collect { case ld: LatencyDemand => ld }
        message match {
          case ChildLatencyRequest(time) =>
            nodeData.context.parent ! ChildLatencyResponse(nodeData.context.self, time)
          case ChildLatencyResponse(childNode, requestTime) =>
            childNode match {
              case bnd.childNode1 => childNode1Latency =
                Some(Duration.between(requestTime, clock.instant).dividedBy(2))
              case bnd.childNode2 => childNode2Latency =
                Some(Duration.between(requestTime, clock.instant).dividedBy(2))
            }
            if (childNode1Latency.isDefined &&
              childNode2Latency.isDefined &&
              childNode1PathLatency.isDefined &&
              childNode2PathLatency.isDefined) {
              val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
              val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
              if (pathLatency1.compareTo(pathLatency2) >= 0) {
                nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency1)
                if (logging && latencyDemands.nonEmpty)
                  println(
                    s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. " +
                      s"(Calculated every $interval seconds.)")
                updateViolations(pathLatency1, latencyDemands)
              } else {
                nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
                if (logging && latencyDemands.nonEmpty)
                  println(
                    s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. " +
                      s"(Calculated every $interval seconds.)")
                updateViolations(pathLatency2, latencyDemands)
              }
              childNode1Latency = None
              childNode2Latency = None
              childNode1PathLatency = None
              childNode2PathLatency = None
            }
          case PathLatency(childNode, duration) =>
            childNode match {
              case bnd.childNode1 => childNode1PathLatency = Some(duration)
              case bnd.childNode2 => childNode2PathLatency = Some(duration)
            }
            if (childNode1Latency.isDefined &&
              childNode2Latency.isDefined &&
              childNode1PathLatency.isDefined &&
              childNode2PathLatency.isDefined) {
              val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
              val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
              if (pathLatency1.compareTo(pathLatency2) >= 0) {
                nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency1)
                if (logging && latencyDemands.nonEmpty)
                  println(
                    s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency1. " +
                      s"(Calculated every $interval seconds.)")
                updateViolations(pathLatency1, latencyDemands)
              } else {
                nodeData.context.parent ! PathLatency(nodeData.context.self, pathLatency2)
                if (logging && nodeData.query.demands.collect { case ld: LatencyDemand => ld }.nonEmpty)
                  println(
                    s"LATENCY:\tEvents reach node `${nodeData.name}` after $pathLatency2. " +
                      s"(Calculated every $interval seconds.)")
                updateViolations(pathLatency2, latencyDemands)
              }
              childNode1Latency = None
              childNode2Latency = None
              childNode1PathLatency = None
              childNode2PathLatency = None
            }
        }
    }
  }

  override def copy: PathLatencyMonitor = PathLatencyMonitor(interval, logging)

  def isDemandNotMet(latency: Duration, ld: LatencyDemand): Boolean =
    !isFulfilled(TimeSpan(latency.toMillis), ld)
}