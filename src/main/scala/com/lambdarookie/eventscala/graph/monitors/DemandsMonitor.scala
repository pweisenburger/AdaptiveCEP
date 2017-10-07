package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.Utilities
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries._


trait InfoMessage
case class ChildInfoRequest() extends InfoMessage
case class ChildInfoResponse(childNode: ActorRef) extends InfoMessage
case class PathInfo(childNode: ActorRef, measurements: Measurements) extends InfoMessage

case class Measurements(latency: TimeSpan, bandwidth: BitRate, throughput: BitRate) {
  def this(from: Host, to: Host, system: System) = this(system.getLatencyAndUpdatePaths(from, to),
    system.getBandwidthAndUpdatePaths(from, to), system.getThroughputAndUpdatePaths(from, to))

  def combine(other: Measurements): Measurements =
    Measurements(latency + other.latency, min(bandwidth, other.bandwidth), min(throughput, other.throughput))
}

case class DemandsMonitor(interval: Int, logging: Boolean) extends Monitor {

  private var child1Measurements: Option[Measurements] = None
  private var child1PathMeasurements: Option[Measurements] = None
  private var child2Measurements: Option[Measurements] = None
  private var child2PathMeasurements: Option[Measurements] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val childNodes: Seq[ActorRef] = nodeData match {
      case lnd: LeafNodeData =>
        if (lnd.query.demands.nonEmpty && logging)
          println("LOG:\tDemands for leaf nodes are ignored.")
        Seq.empty
      case und: UnaryNodeData => Seq(und.childNode)
      case bnd: BinaryNodeData => Seq(bnd.childNode1, bnd.childNode2)
    }
    if (childNodes.nonEmpty && interval > 0) nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => childNodes.foreach(_ ! ChildInfoRequest()))
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val query: Query = nodeData.query
    val system: System = nodeData.system
    val self: ActorRef = nodeData.context.self
    val operator: Operator = system.nodesToOperators.now.apply(self)
    val host: Host = operator.host

    def updateViolations(pathMeasurements: Measurements): Unit = {
      val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d, pathMeasurements) => d }
      if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, pathMeasurements)
      val violatedDemands: Set[Demand] = demands.filter(isDemandNotMet(pathMeasurements, _))
      query.violations.now.foreach{
        case v @ Violation(_, d) if (demands -- violatedDemands).contains(d) => query.removeViolation(v)
        case _ =>
      }
      val violations: Set[Violation] = violatedDemands.map(Violation(operator, _))
      if (violations.nonEmpty) system.fireDemandsViolated(violations)
    }

    if (message.isInstanceOf[InfoMessage]) nodeData match {
      case _: LeafNodeData => message match {
        case ChildInfoRequest() =>
          nodeData.context.parent ! ChildInfoResponse(self)
          nodeData.context.parent ! PathInfo(self, Measurements(0.ns, Int.MaxValue.gbps, Int.MaxValue.gbps))
      }
      case _: UnaryNodeData =>
        message match {
          case ChildInfoRequest() => nodeData.context.parent ! ChildInfoResponse(self)
          case ChildInfoResponse(childNode) =>
            val childHost: Host = system.getHostByNode(childNode)
            child1Measurements = Some(new Measurements(childHost, host, system))
            if (child1PathMeasurements.isDefined) {
              val pathMeasurements: Measurements = child1Measurements.get combine child1PathMeasurements.get
              updateViolations(pathMeasurements)
              nodeData.context.parent ! PathInfo(self, pathMeasurements)
              child1Measurements = None
              child1PathMeasurements = None
            }
          case PathInfo(_, childNodePathMeasurements) =>
            if (child1Measurements.isDefined) {
              val pathMeasurements: Measurements = child1Measurements.get combine childNodePathMeasurements
              updateViolations(pathMeasurements)
              nodeData.context.parent ! PathInfo(self, pathMeasurements)
              child1Measurements = None
              child1PathMeasurements = None
            }
        }
      case bnd: BinaryNodeData =>
        message match {
          case ChildInfoRequest() => nodeData.context.parent ! ChildInfoResponse(self)
          case ChildInfoResponse(childNode) =>
            val childHost: Host = system.getHostByNode(childNode)
            val childMeasurements: Measurements = new Measurements(childHost, host, system)
            childNode match {
              case bnd.childNode1 => child1Measurements = Some(childMeasurements)
              case bnd.childNode2 => child2Measurements = Some(childMeasurements)
            }
            if (child1PathMeasurements.isDefined && child2PathMeasurements.isDefined &&
              child1Measurements.isDefined && child2Measurements.isDefined) {
              val path1Measurements: Measurements = child1Measurements.get combine child1PathMeasurements.get
              val path2Measurements: Measurements = child2Measurements.get combine child2PathMeasurements.get
              val chosenPathMeasurements: Measurements = choosePaths(path1Measurements, path2Measurements)
              updateViolations(chosenPathMeasurements)
              nodeData.context.parent ! PathInfo(self, chosenPathMeasurements)
              child1Measurements = None
              child2Measurements = None
              child1PathMeasurements = None
              child2PathMeasurements = None
            }
          case PathInfo(childNode, childNodePathMeasurements) =>
            childNode match {
              case bnd.childNode1 => child1PathMeasurements = Some(childNodePathMeasurements)
              case bnd.childNode2 => child2PathMeasurements = Some(childNodePathMeasurements)
            }
            if (child1PathMeasurements.isDefined && child2PathMeasurements.isDefined &&
              child1Measurements.isDefined && child2Measurements.isDefined) {
              val path1Measurements: Measurements = child1Measurements.get combine child1PathMeasurements.get
              val path2Measurements: Measurements = child2Measurements.get combine child2PathMeasurements.get
              val chosenPathMeasurements: Measurements = choosePaths(path1Measurements, path2Measurements)
              updateViolations(chosenPathMeasurements)
              nodeData.context.parent ! PathInfo(self, chosenPathMeasurements)
              child1Measurements = None
              child2Measurements = None
              child1PathMeasurements = None
              child2PathMeasurements = None
            }
        }
    }
  }

  override def copy: DemandsMonitor = DemandsMonitor(interval, logging)

  private def isDemandNotMet(pathMeasurements: Measurements, d: Demand): Boolean = {
    val met: Boolean = d match {
      case ld: LatencyDemand =>
        val latency: TimeSpan = pathMeasurements.latency
        Utilities.isFulfilled(latency, ld)
      case bd: BandwidthDemand =>
        val bandwidth: BitRate = pathMeasurements.bandwidth
        Utilities.isFulfilled(bandwidth, bd)
      case td: ThroughputDemand =>
        val throughput: BitRate = pathMeasurements.throughput
        Utilities.isFulfilled(throughput, td)
    }
    !met
  }

  private def areConditionsMet(demand: Demand, measurements: Measurements): Boolean = {
    if (demand.conditions.exists(_.isInstanceOf[Demand]))
      demand.conditions.foreach { case d: Demand if !isDemandNotMet(measurements, d) => d.notFulfilled = false; case _ => }
    if (demand.conditions.exists(_.notFulfilled)) {
      if (logging) println(s"LOG:\t\tSome conditions for the demand $demand are not met.")
      false
    } else {
      true
    }
  }

  private def logDemands(demands: Set[Demand], name: String, pathMeasurements: Measurements): Unit = {
    if (demands.exists(_.isInstanceOf[LatencyDemand]))
      println(s"LOG:\t\tNode `$name` has a highest latency of ${pathMeasurements.latency.toMillis} ms.")
    if (demands.exists(_.isInstanceOf[BandwidthDemand]))
      println(s"LOG:\t\tNode `$name` has a lowest bandwidth of ${pathMeasurements.bandwidth.toMbps} mbps.")
    if (demands.exists(_.isInstanceOf[ThroughputDemand]))
      println(s"LOG:\t\tNode `$name` has a lowest throughput of ${pathMeasurements.throughput.toMbps} mbps.")
  }

  private def choosePaths(path1Measurements: Measurements, path2Measurements: Measurements): Measurements = {
    Measurements(
      if (path1Measurements.latency >= path2Measurements.latency)
        path1Measurements.latency
      else
        path2Measurements.latency,
      if (path1Measurements.bandwidth < path2Measurements.bandwidth)
        path1Measurements.bandwidth
      else
        path2Measurements.bandwidth,
      if (path1Measurements.throughput < path2Measurements.throughput)
        path1Measurements.throughput
      else
        path2Measurements.throughput
    )
  }
}
