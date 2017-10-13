package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QoSMetrics._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.{Host, Operator, System}

/**
  * Created by monur.
  */
object Utilities {

  def calculateLowestLatency(from: Host, to: Host, knownPaths: Set[Path], visited: Set[Host] = Set()): (Path, TimeSpan) =
    if (from == to) {
      (Path(from, to, Seq.empty), from.neighborLatencies(to))
    } else {
      val foundOption: Option[Path] = knownPaths.collectFirst { case p@Path(`from`, `to`, _) => p }
      if (foundOption.nonEmpty) {
        (foundOption.get, foundOption.get.latency)
      } else {
        val best: Seq[(Path, TimeSpan)] = (from.neighborLatencies -- visited - from).map { h_t =>
          calculateLowestLatency(h_t._1, to, knownPaths, visited + from)
        }.toSeq.sortWith(_._2 < _._2)
        if (best.isEmpty)
          (Path(from, to, Seq.empty), Float.PositiveInfinity.sec)
        else
          (Path(from, to, best.head._1.source +: best.head._1.hops),
            from.neighborLatencies(best.head._1.source) + best.head._2)
      }
    }


  def calculateHighestBandwidth(from: Host, to: Host, knownPaths: Set[Path], visited: Set[Host] = Set()): (Path, BitRate) =
    if (from == to) {
      (Path(from, to, Seq.empty), from.neighborBandwidths(to))
    } else {
      val foundOption: Option[Path] = knownPaths.collectFirst { case p@Path(`from`, `to`, _) => p }
      if (foundOption.nonEmpty) {
        (foundOption.get, foundOption.get.bandwidth)
      } else {
        val best: Seq[(Path, BitRate)] = (from.neighborBandwidths -- visited - from).map { h_t =>
          calculateHighestBandwidth(h_t._1, to, knownPaths, visited + from)
        }.toSeq.sortWith(_._2 > _._2)
        if (best.isEmpty)
          (Path(from, to, Seq.empty), 0.kbps)
        else
          (Path(from, to, best.head._1.source +: best.head._1.hops),
            min(from.neighborBandwidths(best.head._1.source), best.head._2))
      }
    }

  def calculateHighestThroughput(from: Host, to: Host, knownPaths: Set[Path], visited: Set[Host] = Set()): (Path, BitRate) =
    if (from == to) {
      (Path(from, to, Seq.empty), from.neighborThroughputs(to))
    } else {
      val foundOption: Option[Path] = knownPaths.collectFirst { case p@Path(`from`, `to`, _) => p }
      if (foundOption.nonEmpty) {
        (foundOption.get, foundOption.get.throughput)
      } else {
        val best: Seq[(Path, BitRate)] = (from.neighborThroughputs -- visited - from).map { h_t =>
          calculateHighestThroughput(h_t._1, to, knownPaths, visited + from)
        }.toSeq.sortWith(_._2 > _._2)
        if (best.isEmpty)
          (Path(from, to, Seq.empty), 0.kbps)
        else
          (Path(from, to, best.head._1.source +: best.head._1.hops),
            min(from.neighborThroughputs(best.head._1.source), best.head._2))
      }
    }

  /**
    * Calculate the distance from a coordinate to another coordinate
    * @param from Coordinate of type [[Coordinate]]
    * @param to Coordinate of type [[Coordinate]]
    * @return Distance in meters
    */
  def calculateDistance(from: Coordinate, to: Coordinate): Int = {
    import math._

    val R = 6371 // Radius of the earth
    val latDistance = toRadians(from.latitude - to.latitude)
    val lonDistance = toRadians(from.longitude - to.longitude)
    val a = sin(latDistance / 2) * sin(latDistance / 2) +
      cos(toRadians(from.latitude)) * cos(toRadians(to.latitude))* sin(lonDistance / 2) * sin(lonDistance / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1-a))
    val distance = R * c * 1000 // convert to meters
    val height = from.altitude - to.altitude
    sqrt(pow(distance, 2) + pow(height, 2)).toInt
  }

  /**
    * Calculate the latency on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated latency as [[TimeSpan]]
    */
  def calculateLatency(path: Seq[Host]): TimeSpan = if (path.nonEmpty && path.tail.nonEmpty)
    path.head.neighborLatencies(path(1)) + calculateLatency(path.tail)
  else
    0.ms

  /**
    * Calculate the bandwidth on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated bandwidth as [[BitRate]]
    */
  def calculateBandwidth(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborBandwidths(path(1)), calculateBandwidth(path.tail))
  else
    Int.MaxValue.gbps

  /**
    * Calculate the throughput on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated throughput as [[BitRate]]
    */
  def calculateThroughput(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborThroughputs(path(1)), calculateThroughput(path.tail))
  else
    Int.MaxValue.gbps

  def isFulfilled[T <: QoSUnit[T]](value: QoSUnit[T], condition: Condition): Boolean = condition match {
    case fc: FrequencyCondition =>
      if (!value.isInstanceOf[Ratio]) throw new IllegalArgumentException("QoS metric does not match the condition.")
      val frequency: Ratio = value.asInstanceOf[Ratio]
      fc.booleanOperator match {
        case Equal =>        frequency == fc.ratio
        case NotEqual =>     frequency != fc.ratio
        case Greater =>      frequency > fc.ratio
        case GreaterEqual => frequency >= fc.ratio
        case Smaller =>      frequency < fc.ratio
        case SmallerEqual => frequency <= fc.ratio
      }
    case pc: ProximityCondition =>
      if (!value.isInstanceOf[Distance]) throw new IllegalArgumentException("QoS metric does not match the condition.")
      val proximity: Distance = value.asInstanceOf[Distance]
      pc.booleanOperator match {
        case Equal =>        proximity == pc.distance
        case NotEqual =>     proximity != pc.distance
        case Greater =>      proximity > pc.distance
        case GreaterEqual => proximity >= pc.distance
        case Smaller =>      proximity < pc.distance
        case SmallerEqual => proximity <= pc.distance
      }
    case lc: LatencyDemand =>
      if (!value.isInstanceOf[TimeSpan]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val latency: TimeSpan = value.asInstanceOf[TimeSpan]
      lc.booleanOperator match {
        case Equal =>        latency == lc.timeSpan
        case NotEqual =>     latency != lc.timeSpan
        case Greater =>      latency > lc.timeSpan
        case GreaterEqual => latency >= lc.timeSpan
        case Smaller =>      latency < lc.timeSpan
        case SmallerEqual => latency <= lc.timeSpan
      }
    case bd: BandwidthDemand =>
      if (!value.isInstanceOf[BitRate]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val bandwidth: BitRate = value.asInstanceOf[BitRate]
      bd.booleanOperator match {
        case Equal =>        bandwidth == bd.bitRate
        case NotEqual =>     bandwidth != bd.bitRate
        case Greater =>      bandwidth > bd.bitRate
        case GreaterEqual => bandwidth >= bd.bitRate
        case Smaller =>      bandwidth < bd.bitRate
        case SmallerEqual => bandwidth <= bd.bitRate
      }
    case td: ThroughputDemand =>
      if (!value.isInstanceOf[BitRate]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val throughput: BitRate = value.asInstanceOf[BitRate]
      td.booleanOperator match {
        case Equal =>        throughput == td.bitRate
        case NotEqual =>     throughput != td.bitRate
        case Greater =>      throughput > td.bitRate
        case GreaterEqual => throughput >= td.bitRate
        case Smaller =>      throughput < td.bitRate
        case SmallerEqual => throughput <= td.bitRate
      }
  }

  def getDescendants(operator: Operator): Set[Operator] =
    operator.inputs.toSet ++ operator.inputs.flatMap(getDescendants)

  def violatesNewDemands(system: System, oldHost: Host, newHost: Host, operators: Set[Operator]): Boolean = {
    val operatorsToDemands: Map[Operator, Set[Demand]] = operators.collect {
      case o if o.query.demands.nonEmpty => o -> o.query.demands
    }.toMap
    operatorsToDemands.exists { o_d =>
      o_d._2 exists {
        case ld: LatencyDemand =>
          Utilities.isFulfilled(system.getLatencyAndUpdatePaths(oldHost, o_d._1.host), ld) &&
            !Utilities.isFulfilled(system.getLatencyAndUpdatePaths(newHost, o_d._1.host), ld)
        case bd: BandwidthDemand =>
          Utilities.isFulfilled(system.getBandwidthAndUpdatePaths(oldHost, o_d._1.host), bd) &&
            !Utilities.isFulfilled(system.getBandwidthAndUpdatePaths(newHost, o_d._1.host), bd)
        case td: ThroughputDemand =>
          Utilities.isFulfilled(system.getThroughputAndUpdatePaths(oldHost, o_d._1.host), td) &&
            !Utilities.isFulfilled(system.getThroughputAndUpdatePaths(newHost, o_d._1.host), td)
      }
    }
  }

  /**
    * Used to see how long a method takes
    * @param block Method under test
    * @tparam R Output type of the method
    * @return The output of the method
    */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
}
