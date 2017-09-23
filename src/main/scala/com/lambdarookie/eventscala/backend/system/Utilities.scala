package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QoSMetrics._
import com.lambdarookie.eventscala.backend.system.traits.Host

/**
  * Created by monur.
  */
object Utilities {
  /**
    * Calculate the path with the lowest latency between two hosts using Dijkstra's shortest path algorithm
    * @param from Source host
    * @param to Destination host
    * @return A tuple of [[Latency]](from, to, hops), hops being the hosts between from and to (if any), and the
    *         duration of type [[TimeSpan]].
    */
  def calculateLowestLatency(from: Host, to: Host): (Latency, TimeSpan) = if (from == to) {
    (Latency(from, to, Seq.empty), from.neighborLatencies(to))
  } else {
    var visited: Set[Host] = Set(from)
    var latencies: Map[Host, (Seq[Host], TimeSpan)] =
      from.neighbors.map(n => n -> (Seq.empty, from.neighborLatencies(n))).toMap
    while (latencies.nonEmpty) {
      latencies = latencies.toSeq.sortWith(_._2._2 < _._2._2).toMap
      val next: (Host, (Seq[Host], TimeSpan)) = latencies.head
      if (!visited.contains(next._1))
        if (next._1 == to) {
          return (Latency(from, to, next._2._1), next._2._2)
        } else {
          visited += next._1
          val hops: Seq[Host] = next._2._1 :+ next._1
          next._1.neighborLatencies.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnLatency: TimeSpan = latencies(next._1)._2 + nn._2
            if (!latencies.contains(nn._1) || latencies(nn._1)._2 > sourceToNnLatency)
              latencies += nn._1 -> (hops, sourceToNnLatency)
          })
          latencies -= next._1
        }
    }
    throw new RuntimeException("ERROR:\tMethod should never reach here.")
  }

  /**
    * Calculate the path with the highest bandwidth between two hosts using a modified Dijkstra's shortest path algorithm
    * @param from Source host
    * @param to Destination host
    * @return A tuple of [[Bandwidth]](from, to, hops), hops being the hosts between from and to (if any), and the
    *         bandwidth of type [[BitRate]].
    */
  def calculateHighestBandwidth(from: Host, to: Host): (Bandwidth, BitRate) = if (from == to) {
    (Bandwidth(from, to, Seq.empty), from.neighborBandwidths(to))
  } else {
    var visited: Set[Host] = Set(from)
    var bandwidths: Map[Host, (Seq[Host], BitRate)] =
      from.neighbors.map(n => n -> (Seq.empty, from.neighborBandwidths(n))).toMap
    while (bandwidths.nonEmpty) {
      bandwidths = bandwidths.toSeq.sortWith(_._2._2 > _._2._2).toMap
      val next: (Host, (Seq[Host], BitRate)) = bandwidths.head
      if (!visited.contains(next._1))
        if (next._1 == to) {
          return (Bandwidth(from, to, next._2._1), next._2._2)
        } else {
          visited += next._1
          val hops: Seq[Host] = next._2._1 :+ next._1
          next._1.neighborBandwidths.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnBandwidth: BitRate = min(bandwidths(next._1)._2, nn._2)
            if (!bandwidths.contains(nn._1) || bandwidths(nn._1)._2 < sourceToNnBandwidth)
              bandwidths += nn._1 -> (hops, sourceToNnBandwidth)
          })
          bandwidths -= next._1
        }
    }
    throw new RuntimeException("ERROR:\tMethod should never reach here.")
  }

  /**
    * Calculate the path with the highest throughput between two hosts using a modified Dijkstra's shortest path algorithm
    * @param from Source host
    * @param to Destination host
    * @return A tuple of [[Throughput]](from, to, hops), hops being the hosts between from and to (if any), and the
    *         throughput of type [[BitRate]].
    */
  def calculateHighestThroughput(from: Host, to: Host): (Throughput, BitRate) = if (from == to) {
    (Throughput(from, to, Seq.empty), from.neighborThroughputs(to))
  } else {
    var visited: Set[Host] = Set(from)
    var throughputs: Map[Host, (Seq[Host], BitRate)] =
      from.neighbors.map(n => n -> (Seq.empty, from.neighborThroughputs(n))).toMap
    while (throughputs.nonEmpty) {
      throughputs = throughputs.toSeq.sortWith(_._2._2 > _._2._2).toMap
      val next: (Host, (Seq[Host], BitRate)) = throughputs.head
      if (!visited.contains(next._1))
        if (next._1 == to) {
          return (Throughput(from, to, next._2._1), next._2._2)
        } else {
          visited += next._1
          val hops: Seq[Host] = next._2._1 :+ next._1
          next._1.neighborThroughputs.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnThroughput: BitRate = min(throughputs(next._1)._2, nn._2)
            if (!throughputs.contains(nn._1) || throughputs(nn._1)._2 < sourceToNnThroughput)
              throughputs += nn._1 -> (hops, sourceToNnThroughput)
          })
          throughputs -= next._1
        }
    }
    throw new RuntimeException("ERROR:\tMethod should never reach here.")
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
