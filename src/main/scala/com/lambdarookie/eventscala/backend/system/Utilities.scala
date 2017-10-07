package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QoSMetrics._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.{Host, Operator}

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
          (Path(from, to, Seq.empty), Int.MaxValue.sec)
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
            from.neighborBandwidths(best.head._1.source) + best.head._2)
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
            from.neighborThroughputs(best.head._1.source) + best.head._2)
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
    case FrequencyCondition(bo, r) =>
      if (!value.isInstanceOf[Ratio]) throw new IllegalArgumentException("QoS metric does not match the condition.")
      val frequency: Ratio = value.asInstanceOf[Ratio]
      bo match {
        case Equal =>        frequency == r
        case NotEqual =>     frequency != r
        case Greater =>      frequency > r
        case GreaterEqual => frequency >= r
        case Smaller =>      frequency < r
        case SmallerEqual => frequency <= r
      }
    case ProximityCondition(bo, d) =>
      if (!value.isInstanceOf[Distance]) throw new IllegalArgumentException("QoS metric does not match the condition.")
      val proximity: Distance = value.asInstanceOf[Distance]
      bo match {
        case Equal =>        proximity == d
        case NotEqual =>     proximity != d
        case Greater =>      proximity > d
        case GreaterEqual => proximity >= d
        case Smaller =>      proximity < d
        case SmallerEqual => proximity <= d
      }
    case LatencyDemand(bo, ts, _) =>
      if (!value.isInstanceOf[TimeSpan]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val latency: TimeSpan = value.asInstanceOf[TimeSpan]
      bo match {
        case Equal =>        latency == ts
        case NotEqual =>     latency != ts
        case Greater =>      latency > ts
        case GreaterEqual => latency >= ts
        case Smaller =>      latency < ts
        case SmallerEqual => latency <= ts
      }
    case BandwidthDemand(bo, br, _) =>
      if (!value.isInstanceOf[BitRate]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val bandwidth: BitRate = value.asInstanceOf[BitRate]
      bo match {
        case Equal =>        bandwidth == br
        case NotEqual =>     bandwidth != br
        case Greater =>      bandwidth > br
        case GreaterEqual => bandwidth >= br
        case Smaller =>      bandwidth < br
        case SmallerEqual => bandwidth <= br
      }
    case ThroughputDemand(bo, br, _) =>
      if (!value.isInstanceOf[BitRate]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val throughput: BitRate = value.asInstanceOf[BitRate]
      bo match {
        case Equal =>        throughput == br
        case NotEqual =>     throughput != br
        case Greater =>      throughput > br
        case GreaterEqual => throughput >= br
        case Smaller =>      throughput < br
        case SmallerEqual => throughput <= br
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
