package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.system.traits.Host

/**
  * Created by monur.
  */
object Utilities {
  /**
    * Calculate the path with the lowest latency between two hosts using Dijkstra's shortest path algorithm
    * @param source Source host
    * @param dest Destination host
    * @return The tuple of the path (For the path 'source -> A -> B -> dest' returns 'Seq(A, B, dest)') and the latency
    */
  def calculateLowestLatency(source: Host, dest: Host): (Seq[Host], TimeSpan) = if (source == dest) {
    source.neighborLatencies(dest)
  } else {
    var visited: Set[Host] = Set(source)
    var latencies: Map[Host, (Seq[Host], TimeSpan)] = source.neighborLatencies - source
    while (latencies.nonEmpty) {
      latencies = latencies.toSeq.sortWith(_._2._2 < _._2._2).toMap
      val n: (Host, (Seq[Host], TimeSpan)) = latencies.head
      if (!visited.contains(n._1))
        if (n._1 == dest) {
          return (n._2._1 :+ dest, n._2._2)
        } else {
          visited += n._1
          val inters: Seq[Host] = n._2._1 :+ n._1
          n._1.neighborLatencies.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnLatency: TimeSpan = latencies(n._1)._2 + nn._2._2
            if (!latencies.contains(nn._1) || latencies(nn._1)._2 > sourceToNnLatency)
              latencies += nn._1 -> (inters, sourceToNnLatency)
          })
          latencies -= n._1
        }
    }
    throw new RuntimeException("ERROR:\tMethod should never reach here.")
  }

  /**
    * Calculate the path with the highest bandwidth between two hosts using a modified Dijkstra's shortest path algorithm
    * @param source Source host
    * @param dest Destination host
    * @return The tuple of the path (For the path 'source -> A -> B -> dest' returns 'Seq(A, B, dest)') and the bandwidth
    */
  def calculateHighestBandwidth(source: Host, dest: Host): (Seq[Host], BitRate) = if (source == dest) {
    source.neighborBandwidths(dest)
  } else {
    var visited: Set[Host] = Set(source)
    var bandwidths: Map[Host, (Seq[Host], BitRate)] = source.neighborBandwidths - source
    while (bandwidths.nonEmpty) {
      bandwidths = bandwidths.toSeq.sortWith(_._2._2 > _._2._2).toMap
      val n: (Host, (Seq[Host], BitRate)) = bandwidths.head
      if (!visited.contains(n._1))
        if (n._1 == dest) {
          return (n._2._1 :+ dest, n._2._2)
        } else {
          visited += n._1
          val inters: Seq[Host] = n._2._1 :+ n._1
          n._1.neighborBandwidths.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnBandwidth: BitRate = min(bandwidths(n._1)._2, nn._2._2)
            if (!bandwidths.contains(nn._1) || bandwidths(nn._1)._2 < sourceToNnBandwidth)
              bandwidths += nn._1 -> (inters, sourceToNnBandwidth)
          })
          bandwidths -= n._1
        }
    }
    throw new RuntimeException("ERROR:\tMethod should never reach here.")
  }

  /**
    * Calculate the path with the highest throughput between two hosts using a modified Dijkstra's shortest path algorithm
    * @param source Source host
    * @param dest Destination host
    * @return The tuple of the path (For the path 'source -> A -> B -> dest' returns 'Seq(A, B, dest)') and the throughput
    */
  def calculateHighestThroughput(source: Host, dest: Host): (Seq[Host], BitRate) = if (source == dest) {
    source.neighborThroughputs(dest)
  } else {
    var visited: Set[Host] = Set(source)
    var throughputs: Map[Host, (Seq[Host], BitRate)] = source.neighborThroughputs - source
    while (throughputs.nonEmpty) {
      throughputs = throughputs.toSeq.sortWith(_._2._2 > _._2._2).toMap
      val n: (Host, (Seq[Host], BitRate)) = throughputs.head
      if (!visited.contains(n._1))
        if (n._1 == dest) {
          return (n._2._1 :+ dest, n._2._2)
        } else {
          visited += n._1
          val inters: Seq[Host] = n._2._1 :+ n._1
          n._1.neighborThroughputs.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnThroughput: BitRate = min(throughputs(n._1)._2, nn._2._2)
            if (!throughputs.contains(nn._1) || throughputs(nn._1)._2 < sourceToNnThroughput)
              throughputs += nn._1 -> (inters, sourceToNnThroughput)
          })
          throughputs -= n._1
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
    path.head.neighborLatencies(path(1))._2 + calculateLatency(path.tail)
  else
    0.ms

  /**
    * Calculate the bandwidth on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated bandwidth as [[BitRate]]
    */
  def calculateBandwidth(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborBandwidths(path(1))._2, calculateBandwidth(path.tail))
  else
    Int.MaxValue.gbps

  /**
    * Calculate the throughput on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated throughput as [[BitRate]]
    */
  def calculateThroughput(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborThroughputs(path(1))._2, calculateThroughput(path.tail))
  else
    Int.MaxValue.gbps
}