package com.lambdarookie.eventscala.graph.monitors

import java.time._
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries._


trait InfoMessage
case class ChildInfoRequest() extends InfoMessage
case class ChildInfoResponse(childNode: ActorRef) extends InfoMessage
case class PathInfoMessage(childNode: ActorRef, pathInfo: PathInfo) extends InfoMessage

case class PathInfo(latencyInfo: LatencyInfo, bandwidthInfo: BandwidthInfo, throughputInfo: ThroughputInfo)
case class LatencyInfo(path: Seq[Host], latency: Duration)
case class BandwidthInfo(path: Seq[Host], bandwidth: BitRate)
case class ThroughputInfo(path: Seq[Host], throughput: BitRate)

sealed trait Priority
case object LatencyPriority extends Priority
case object BandwidthPriority extends Priority
case object ThroughputPriority extends Priority

case class DemandsMonitor(messageInterval: Int, latencyInterval: Int, bandwidthInterval: Int, throughputInterval: Int,
                          priority: Priority, logging: Boolean) extends Monitor {

  val clock: Clock = Clock.systemDefaultZone

  var child1ChosenPath: Option[Seq[Host]] = None
  var child1PathInfo: Option[PathInfo] = None
  var child2ChosenPath: Option[Seq[Host]] = None
  var child2PathInfo: Option[PathInfo] = None


  override def onCreated(nodeData: NodeData): Unit = {
    val childNodes: Seq[ActorRef] = nodeData match {
      case lnd: LeafNodeData =>
        if (lnd.query.demands.nonEmpty && logging)
          println("LOG:\tDemands for leaf nodes are ignored.")
        Seq.empty
      case und: UnaryNodeData => Seq(und.childNode)
      case bnd: BinaryNodeData => Seq(bnd.childNode1, bnd.childNode2)
    }
    val host: Host = nodeData.system.getHostByNode(nodeData.context.self)
    if (childNodes.nonEmpty) nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(messageInterval, TimeUnit.SECONDS),
      interval = FiniteDuration(messageInterval, TimeUnit.SECONDS),
      runnable = () => {
        childNodes.foreach(_ ! ChildInfoRequest())
      })
    nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(messageInterval, TimeUnit.SECONDS),
      interval = FiniteDuration(latencyInterval, TimeUnit.SECONDS),
      runnable = () => {
        host.measureNeighborLatencies()
      })
    nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(messageInterval, TimeUnit.SECONDS),
      interval = FiniteDuration(bandwidthInterval, TimeUnit.SECONDS),
      runnable = () => {
        host.measureNeighborBandwidths()
      })
    nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(messageInterval, TimeUnit.SECONDS),
      interval = FiniteDuration(throughputInterval, TimeUnit.SECONDS),
      runnable = () => {
        host.measureNeighborThroughputs()
      })
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val query: Query = nodeData.query
    val system: System = nodeData.system
    val self: ActorRef = nodeData.context.self
    val operator: Operator = system.nodesToOperators.now.apply(self)
    val host: Host = operator.host

    def createPathInfo(childNodeChosenPath: Option[Seq[Host]], childNodePathInfo: Option[PathInfo]): PathInfo = {
      val path = childNodePathInfo.get.latencyInfo.path ++ childNodeChosenPath.get
      PathInfo(LatencyInfo(path, system.calculateLatency(path).toDuration),
        BandwidthInfo(path, system.calculateBandwidth(path)), ThroughputInfo(path, system.calculateThroughput(path)))
    }

    def getChosenPathFromChild(childHost: Host): Seq[Host] = priority match {
        case LatencyPriority => system.calculateLowestLatency(childHost, host)._1
        case BandwidthPriority => system.calculateHighestBandwidth(childHost, host)._1
        case ThroughputPriority => system.calculateHighestThroughput(childHost, host)._1
      }

    if (message.isInstanceOf[InfoMessage]) nodeData match {
      case _: LeafNodeData => message match {
        case ChildInfoRequest() =>
          nodeData.context.parent ! ChildInfoResponse(self)
          nodeData.context.parent !
            PathInfoMessage(self, PathInfo(LatencyInfo(Seq(host), Duration.ZERO),
              BandwidthInfo(Seq(host), host.maxBandwidth), ThroughputInfo(Seq(host), host.maxBandwidth)))
      }
      case _: UnaryNodeData =>
        message match {
          case ChildInfoRequest() => nodeData.context.parent ! ChildInfoResponse(self)
          case ChildInfoResponse(childNode) =>
            val childHost: Host = system.getHostByNode(childNode)
            child1ChosenPath = Some(getChosenPathFromChild(childHost))
            if (child1PathInfo.isDefined) {
              val pathInfo: PathInfo = createPathInfo(child1ChosenPath, child1PathInfo)
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! PathInfoMessage(self, pathInfo)
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, pathInfo)
              demands.foreach(d => if (isDemandNotMet(pathInfo, d)) system.fireDemandViolated(Violation(operator, d)))
              child1ChosenPath = None
              child1PathInfo = None
            }
          case PathInfoMessage(_, childNodePathInfo) =>
            if (child1ChosenPath.isDefined) {
              val pathInfo: PathInfo = createPathInfo(child1ChosenPath, Some(childNodePathInfo))
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! PathInfoMessage(self, pathInfo)
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, pathInfo)
              demands.foreach(d => if (isDemandNotMet(pathInfo, d)) system.fireDemandViolated(Violation(operator, d)))
              child1ChosenPath = None
              child1PathInfo = None
            }
        }
      case bnd: BinaryNodeData =>
        message match {
          case ChildInfoRequest() => nodeData.context.parent ! ChildInfoResponse(self)
          case ChildInfoResponse(childNode) =>
            val childHost: Host = system.getHostByNode(childNode)
            val childChosenPath: Seq[Host] = getChosenPathFromChild(childHost)
            childNode match {
              case bnd.childNode1 => child1ChosenPath = Some(childChosenPath)
              case bnd.childNode2 => child2ChosenPath = Some(childChosenPath)
            }
            if (child1PathInfo.isDefined && child2PathInfo.isDefined &&
              child1ChosenPath.isDefined && child2ChosenPath.isDefined) {
              val path1Info: PathInfo = createPathInfo(child1ChosenPath, child1PathInfo)
              val path2Info: PathInfo = createPathInfo(child2ChosenPath, child2PathInfo)
              val chosenPathInfo: PathInfo = choosePaths(path1Info, path2Info)
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! PathInfoMessage(self, chosenPathInfo)
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, chosenPathInfo)
              demands.foreach(d =>
                if (isDemandNotMet(chosenPathInfo, d)) system.fireDemandViolated(Violation(operator, d)))
              child1ChosenPath = None
              child2ChosenPath = None
              child1PathInfo = None
              child2PathInfo = None
            }
          case PathInfoMessage(childNode, childNodePathInfo) =>
            childNode match {
              case bnd.childNode1 => child1PathInfo = Some(childNodePathInfo)
              case bnd.childNode2 => child2PathInfo = Some(childNodePathInfo)
            }
            if (child1PathInfo.isDefined && child2PathInfo.isDefined &&
              child1ChosenPath.isDefined && child2ChosenPath.isDefined) {
              val path1Info: PathInfo = createPathInfo(child1ChosenPath, child1PathInfo)
              val path2Info: PathInfo = createPathInfo(child2ChosenPath, child2PathInfo)
              val chosenPathsInfo: PathInfo = choosePaths(path1Info, path2Info)
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! PathInfoMessage(self, chosenPathsInfo)
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, chosenPathsInfo)
              demands.foreach(d =>
                if (isDemandNotMet(chosenPathsInfo, d)) system.fireDemandViolated(Violation(operator, d)))
              child1ChosenPath = None
              child2ChosenPath = None
              child1PathInfo = None
              child2PathInfo = None
            }
        }
    }
  }

  override def copy: DemandsMonitor =
    DemandsMonitor(messageInterval, latencyInterval, bandwidthInterval, throughputInterval, priority, logging)

  private def isDemandNotMet(pathInfo: PathInfo, d: Demand): Boolean = {
    val met: Boolean = d match {
      case ld: LatencyDemand =>
        val latency: Duration = pathInfo.latencyInfo.latency
        ld.booleanOperator match {
          case Equal =>        latency.compareTo(ld.timeSpan.toDuration) == 0
          case NotEqual =>     latency.compareTo(ld.timeSpan.toDuration) != 0
          case Greater =>      latency.compareTo(ld.timeSpan.toDuration) >  0
          case GreaterEqual => latency.compareTo(ld.timeSpan.toDuration) >= 0
          case Smaller =>      latency.compareTo(ld.timeSpan.toDuration) <  0
          case SmallerEqual => latency.compareTo(ld.timeSpan.toDuration) <= 0
        }
      case bd: BandwidthDemand =>
        val bandwidth: BitRate = pathInfo.bandwidthInfo.bandwidth
        bd.booleanOperator match {
          case Equal =>         bandwidth == bd.bitRate
          case NotEqual =>      bandwidth != bd.bitRate
          case Greater =>       bandwidth > bd.bitRate
          case GreaterEqual =>  bandwidth >= bd.bitRate
          case Smaller =>       bandwidth < bd.bitRate
          case SmallerEqual =>  bandwidth <= bd.bitRate
        }
      case td: ThroughputDemand =>
        val throughput: BitRate = pathInfo.throughputInfo.throughput
        td.booleanOperator match {
          case Equal =>         throughput == td.bitRate
          case NotEqual =>      throughput != td.bitRate
          case Greater =>       throughput > td.bitRate
          case GreaterEqual =>  throughput >= td.bitRate
          case Smaller =>       throughput < td.bitRate
          case SmallerEqual =>  throughput <= td.bitRate
        }
    }
    !met
  }

  private def areConditionsMet(d: Demand): Boolean = if(d.conditions.exists(_.notFulfilled)) {
    if (logging)
      println("LOG:\tSome conditions for the demand are not met.")
    false
  } else true

  private def logDemands(demands: Set[Demand], name: String, pathInfo: PathInfo): Unit = {
    if (demands.exists(_.isInstanceOf[LatencyDemand]))
      println(s"LOG:\t\tNode `$name` has a highest latency of ${pathInfo.latencyInfo.latency.toMillis} ms " +
        s"on the path ${pathInfo.latencyInfo.path}.")
    if (demands.exists(_.isInstanceOf[BandwidthDemand]))
      println(s"LOG:\t\tNode `$name` has a lowest bandwidth of ${pathInfo.bandwidthInfo.bandwidth.toMbps} mbps " +
        s"on the path ${pathInfo.bandwidthInfo.path}.")
    if (demands.exists(_.isInstanceOf[ThroughputDemand]))
      println(s"LOG:\t\tNode `$name` has a lowest throughput of ${pathInfo.throughputInfo.throughput.toMbps} mbps " +
        s"on the path ${pathInfo.throughputInfo.path}.")
  }

  private def choosePaths(path1Info: PathInfo, path2Info: PathInfo): PathInfo = {
    PathInfo(
      if (path1Info.latencyInfo.latency.compareTo(path2Info.latencyInfo.latency) >= 0)
        path1Info.latencyInfo
      else
        path2Info.latencyInfo,
      if (path1Info.bandwidthInfo.bandwidth < path2Info.bandwidthInfo.bandwidth)
        path1Info.bandwidthInfo
      else
        path2Info.bandwidthInfo,
      if (path1Info.throughputInfo.throughput < path2Info.throughputInfo.throughput)
        path1Info.throughputInfo
      else
        path2Info.throughputInfo
    )
  }
}
