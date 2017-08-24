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


case class ChildInfoRequest(time: Instant)
case class ChildInfoResponse(childNode: ActorRef, requestTime: Instant)
case class PathInfos(childNode: ActorRef,
                     latencyInfo: LatencyInfo, bandwidthInfo: BandwidthInfo, throughputInfo: ThroughputInfo)
case class LatencyInfo(path: Seq[Host], latency: Duration) {
  def this(info: (Seq[Host], TimeSpan)) = this(info._1, info._2.toDuration)
}
case class BandwidthInfo(path: Seq[Host], bandwidth: BitRate) {
  def this(info: (Seq[Host], BitRate)) = this(info._1, info._2)
}
case class ThroughputInfo(path: Seq[Host], throughput: BitRate) {
  def this(info: (Seq[Host], BitRate)) = this(info._1, info._2)
}


case class PathDemandsMonitor(messageInterval: Int, latencyInterval: Int, bandwidthInterval: Int, throughputInterval: Int,
                              logging: Boolean, testing: Boolean) extends NodeMonitor {

  val clock: Clock = Clock.systemDefaultZone

  var childNode1Infos: Option[(LatencyInfo, BandwidthInfo, ThroughputInfo)] = None
  var childNode1PathInfos: Option[(LatencyInfo, BandwidthInfo, ThroughputInfo)] = None
  var childNode2Infos: Option[(LatencyInfo, BandwidthInfo, ThroughputInfo)] = None
  var childNode2PathInfos: Option[(LatencyInfo, BandwidthInfo, ThroughputInfo)] = None


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
        childNodes.foreach(_ ! ChildInfoRequest(clock.instant))
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
    def createPathInfos(childNodeInfos: (LatencyInfo, BandwidthInfo, ThroughputInfo),
                        childNodePathInfos: (LatencyInfo, BandwidthInfo, ThroughputInfo)): PathInfos = PathInfos(self,
      LatencyInfo(childNodePathInfos._1.path ++ childNodeInfos._1.path,
        childNodeInfos._1.latency.plus(childNodePathInfos._1.latency)),
      BandwidthInfo(childNodePathInfos._2.path ++ childNodeInfos._2.path,
        min(childNodeInfos._2.bandwidth, childNodePathInfos._2.bandwidth)),
      ThroughputInfo(childNodePathInfos._3.path ++ childNodeInfos._3.path,
        min(childNodeInfos._3.throughput, childNodePathInfos._3.throughput)))
    nodeData match {
      case _: LeafNodeData => message match {
        case ChildInfoRequest(requestTime) =>
          nodeData.context.parent ! ChildInfoResponse(self, requestTime)
          val path: Seq[Host] = Seq(host)
          nodeData.context.parent ! PathInfos(self,
            LatencyInfo(path, Duration.ZERO), BandwidthInfo(path, host.maxBandwidth), ThroughputInfo(path, host.maxBandwidth))
      }
      case _: UnaryNodeData =>
        message match {
          case ChildInfoRequest(time) => nodeData.context.parent ! ChildInfoResponse(self, time)
          case ChildInfoResponse(childNode, requestTime) =>
            val childHost: Host = system.getHostByNode(childNode)
            childNode1Infos = Some((
              if (testing) new LatencyInfo(system.calculateLowestLatency(childHost, host))
              else LatencyInfo(Seq.empty, Duration.between(requestTime, clock.instant).dividedBy(2)),
              new BandwidthInfo(system.calculateHighestBandwidth(childHost, host)),
              new ThroughputInfo(system.calculateHighestThroughput(childHost, host))))
            if (childNode1PathInfos.isDefined) {
              val pathInfos: PathInfos = createPathInfos(childNode1Infos.get, childNode1PathInfos.get)
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! pathInfos
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, pathInfos)
              demands.foreach(d => if (isDemandNotMet(pathInfos, d)) system.fireDemandViolated(Violation(operator, d)))
              childNode1Infos = None
              childNode1PathInfos = None
            }
          case PathInfos(_, latencyInfo, bandwidthInfo, throughputInfo) =>
            childNode1PathInfos = Some(latencyInfo, bandwidthInfo, throughputInfo)
              if (childNode1Infos.isDefined) {
              val pathInfos: PathInfos = createPathInfos(childNode1Infos.get, childNode1PathInfos.get)
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! pathInfos
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, pathInfos)
              demands.foreach(d => if (isDemandNotMet(pathInfos, d)) system.fireDemandViolated(Violation(operator, d)))
              childNode1Infos = None
              childNode1PathInfos = None
            }
        }
      case bnd: BinaryNodeData =>
        message match {
          case ChildInfoRequest(time) => nodeData.context.parent ! ChildInfoResponse(self, time)
          case ChildInfoResponse(childNode, requestTime) =>
          val childHost: Host = system.getHostByNode(childNode)
            val childNodeValues: (LatencyInfo, BandwidthInfo, ThroughputInfo) = (
              if (testing) new LatencyInfo(system.calculateLowestLatency(childHost, host))
              else LatencyInfo(Seq.empty, Duration.between(requestTime, clock.instant).dividedBy(2)),
              new BandwidthInfo(system.calculateHighestBandwidth(childHost, host)),
              new ThroughputInfo(system.calculateHighestThroughput(childHost, host)))
            childNode match {
              case bnd.childNode1 => childNode1Infos = Some(childNodeValues)
              case bnd.childNode2 => childNode2Infos = Some(childNodeValues)
            }
            if (childNode1Infos.isDefined && childNode2Infos.isDefined &&
              childNode1PathInfos.isDefined && childNode2PathInfos.isDefined) {
              val path1Infos: PathInfos = createPathInfos(childNode1Infos.get, childNode1PathInfos.get)
              val path2Infos: PathInfos = createPathInfos(childNode2Infos.get, childNode2PathInfos.get)
              val slowPathInfos: PathInfos =
                if (path1Infos.latencyInfo.latency.compareTo(path2Infos.latencyInfo.latency) >= 0) path1Infos
                else path2Infos
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! slowPathInfos
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, slowPathInfos)
              demands.foreach(d =>
                if (isDemandNotMet(slowPathInfos, d)) system.fireDemandViolated(Violation(operator, d)))
              childNode1Infos = None
              childNode2Infos = None
              childNode1PathInfos = None
              childNode2PathInfos = None
            }
          case PathInfos(childNode, latencyInfo, bandwidthInfo, throughputInfo) =>
          childNode match {
            case bnd.childNode1 => childNode1PathInfos = Some(latencyInfo, bandwidthInfo, throughputInfo)
            case bnd.childNode2 => childNode2PathInfos = Some(latencyInfo, bandwidthInfo, throughputInfo)
          }
            if (childNode1Infos.isDefined && childNode2Infos.isDefined &&
              childNode1PathInfos.isDefined && childNode2PathInfos.isDefined) {
              val path1Infos: PathInfos = createPathInfos(childNode1Infos.get, childNode1PathInfos.get)
              val path2Infos: PathInfos = createPathInfos(childNode2Infos.get, childNode2PathInfos.get)
              val slowPathInfos: PathInfos =
                if (path1Infos.latencyInfo.latency.compareTo(path2Infos.latencyInfo.latency) >= 0) path1Infos
                else path2Infos
              val demands: Set[Demand] = query.demands.collect { case d if areConditionsMet(d) => d }
              nodeData.context.parent ! slowPathInfos
              if (logging && demands.nonEmpty) logDemands(demands, nodeData.name, slowPathInfos)
              demands.foreach(d =>
                if (isDemandNotMet(slowPathInfos, d)) system.fireDemandViolated(Violation(operator, d)))
              childNode1Infos = None
              childNode2Infos = None
              childNode1PathInfos = None
              childNode2PathInfos = None

            }
      }

    }
  }

  private def isDemandNotMet(pathInfos: PathInfos, d: Demand): Boolean = {
    val met: Boolean = d match {
      case ld: LatencyDemand =>
        val latency: Duration = pathInfos.latencyInfo.latency
        ld.booleanOperator match {
          case Equal =>        latency.compareTo(ld.timeSpan.toDuration) == 0
          case NotEqual =>     latency.compareTo(ld.timeSpan.toDuration) != 0
          case Greater =>      latency.compareTo(ld.timeSpan.toDuration) >  0
          case GreaterEqual => latency.compareTo(ld.timeSpan.toDuration) >= 0
          case Smaller =>      latency.compareTo(ld.timeSpan.toDuration) <  0
          case SmallerEqual => latency.compareTo(ld.timeSpan.toDuration) <= 0
        }
      case bd: BandwidthDemand =>
        val bandwidth: BitRate = pathInfos.bandwidthInfo.bandwidth
        bd.booleanOperator match {
          case Equal =>         bandwidth == bd.bitRate
          case NotEqual =>      bandwidth != bd.bitRate
          case Greater =>       bandwidth > bd.bitRate
          case GreaterEqual =>  bandwidth >= bd.bitRate
          case Smaller =>       bandwidth < bd.bitRate
          case SmallerEqual =>  bandwidth <= bd.bitRate
        }
      case td: ThroughputDemand =>
        val throughput: BitRate = pathInfos.throughputInfo.throughput
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

  private def logDemands(demands: Set[Demand], name: String, pathInfos: PathInfos): Unit = {
    if (demands.exists(_.isInstanceOf[LatencyDemand]))
      println(s"LOG:\t\tNode `$name` has a latency of ${pathInfos.latencyInfo.latency.toMillis} ms " +
        s"on the path ${pathInfos.latencyInfo.path}.")
    if (demands.exists(_.isInstanceOf[BandwidthDemand]))
      println(s"LOG:\t\tNode `$name` has a bandwidth of ${pathInfos.bandwidthInfo.bandwidth.toMbps} mbps " +
        s"on the path ${pathInfos.bandwidthInfo.path}.")
    if (demands.exists(_.isInstanceOf[ThroughputDemand]))
      println(s"LOG:\t\tNode `$name` has a throughput of ${pathInfos.throughputInfo.throughput.toMbps} mbps " +
        s"on the path ${pathInfos.throughputInfo.path}.")
  }
}