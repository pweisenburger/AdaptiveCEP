package com.lambdarookie.eventscala.backend.qos

import QoSUnits._
import com.lambdarookie.eventscala.backend.system.traits.Host

/**
  * Created by monur.
  */
object PathFinding {

  case class Path (source: Host, destination: Host, hops: Seq[Host]) {
    def latency: TimeSpan = if (hops.isEmpty)
      source.neighborLatencies(destination)
    else
      calculateLatency(toSeq)

    def bandwidth: BitRate = if (hops.isEmpty)
      source.neighborBandwidths(destination)
    else
      calculateBandwidth(toSeq)

    def throughput: BitRate = if (hops.isEmpty)
      source.neighborThroughputs(destination)
    else
      calculateThroughput(toSeq)

    def toSeq: Seq[Host] = source +: hops :+ destination
  }


  case class Priority(latencyWeight: Int, bandwidthWeight: Int, throughputWeight: Int) {
    def choosePath(from: Host, to: Host, knownPaths: Set[Path]): Path = {
      def weightRate(weight: Float): Float = weight / (latencyWeight + bandwidthWeight + throughputWeight)

      val latency: (Path, TimeSpan) = calculateLowestLatency(from, to, knownPaths)
      val bandwidth: (Path, BitRate) = calculateHighestBandwidth(from, to, knownPaths)
      val throughput: (Path, BitRate) = calculateHighestThroughput(from, to, knownPaths)

      val bandwidthOfLatencyPath: Float = calculateBandwidth(latency._1.toSeq).toKbps
      val throughputOfLatencyPath: Float = calculateThroughput(latency._1.toSeq).toKbps
      val latencyOfBandwidthPath: Float = calculateLatency(bandwidth._1.toSeq).toMillis
      val throughputOfBandwidthPath: Float = calculateThroughput(bandwidth._1.toSeq).toKbps
      val latencyOfThroughputPath: Float = calculateLatency(throughput._1.toSeq).toMillis
      val bandwidthOfThroughputPath: Float = calculateBandwidth(throughput._1.toSeq).toKbps

      val latencyWeightRate: Float = weightRate(latencyWeight)
      val bandwidthWeightRate: Float = weightRate(bandwidthWeight)
      val throughputWeightRate: Float = weightRate(throughputWeight)

      def latencyRate(millis: Float): Float =
        (1f / millis) / (1f / latency._2.toMillis + 1f / latencyOfBandwidthPath + 1f / latencyOfThroughputPath)
      def bandwidthRate(kbps: Float): Float =
        kbps / (bandwidthOfLatencyPath + bandwidth._2.toKbps + bandwidthOfThroughputPath)
      def throughputRate(kbps: Float): Float =
        kbps / (throughputOfLatencyPath + throughputOfBandwidthPath + throughput._2.toKbps)

      val latencyScore: Float = latencyWeightRate * latencyRate(latency._2.toMillis) +
        bandwidthWeightRate * bandwidthRate(bandwidthOfLatencyPath) +
        throughputWeightRate * throughputRate(throughputOfLatencyPath)
      val bandwidthScore: Float = latencyWeightRate * latencyRate(latencyOfBandwidthPath) +
        bandwidthWeightRate * bandwidthRate(bandwidth._2.toKbps) +
        throughputWeightRate * throughputRate(throughputOfBandwidthPath)
      val throughputScore: Float = latencyWeightRate * latencyRate(latencyOfThroughputPath) +
        bandwidthWeightRate * bandwidthRate(bandwidthOfThroughputPath) +
        throughputWeightRate * throughputRate(throughput._2.toKbps)

      if (latencyScore > math.max(bandwidthScore, throughputScore))
        latency._1
      else if (bandwidthScore > math.max(latencyScore, throughputScore))
        bandwidth._1
      else
        throughput._1
    }
  }



  private def calculateLowestLatency(from: Host, to: Host, knownPaths: Set[Path], visited: Set[Host] = Set()): (Path, TimeSpan) =
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

  private def calculateHighestBandwidth(from: Host, to: Host, knownPaths: Set[Path], visited: Set[Host] = Set()): (Path, BitRate) =
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

  private def calculateHighestThroughput(from: Host, to: Host, knownPaths: Set[Path], visited: Set[Host] = Set()): (Path, BitRate) =
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
    * Calculate the latency on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated latency as [[TimeSpan]]
    */
  private def calculateLatency(path: Seq[Host]): TimeSpan = if (path.nonEmpty && path.tail.nonEmpty)
    path.head.neighborLatencies(path(1)) + calculateLatency(path.tail)
  else
    0.ms

  /**
    * Calculate the bandwidth on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated bandwidth as [[BitRate]]
    */
  private def calculateBandwidth(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborBandwidths(path(1)), calculateBandwidth(path.tail))
  else
    Int.MaxValue.gbps

  /**
    * Calculate the throughput on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated throughput as [[BitRate]]
    */
  private def calculateThroughput(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborThroughputs(path(1)), calculateThroughput(path.tail))
  else
    Int.MaxValue.gbps
}
