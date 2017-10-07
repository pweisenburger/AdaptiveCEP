package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.data.QoSUnits.{BitRate, TimeSpan}
import com.lambdarookie.eventscala.backend.system.Utilities
import com.lambdarookie.eventscala.backend.system.traits.Host

/**
  * Created by monur.
  */
object QoSMetrics {
  case class Path (source: Host, destination: Host, hops: Seq[Host]) {
    def latency: TimeSpan = if (hops.isEmpty)
      source.neighborLatencies(destination)
    else
      Utilities.calculateLatency(toSeq)

    def bandwidth: BitRate = if (hops.isEmpty)
      source.neighborBandwidths(destination)
    else
      Utilities.calculateBandwidth(toSeq)

    def throughput: BitRate = if (hops.isEmpty)
      source.neighborThroughputs(destination)
    else
      Utilities.calculateThroughput(toSeq)

    def toSeq: Seq[Host] = source +: hops :+ destination
  }

  case class Priority(latencyWeight: Int, bandwidthWeight: Int, throughputWeight: Int) {
    def choosePath(from: Host, to: Host, knownPaths: Set[Path]): Path = {
      def weightRate(weight: Float): Float = weight / (latencyWeight + bandwidthWeight + throughputWeight)

      val latency: (Path, TimeSpan) = Utilities.calculateLowestLatency(from, to, knownPaths)
      val bandwidth: (Path, BitRate) = Utilities.calculateHighestBandwidth(from, to, knownPaths)
      val throughput: (Path, BitRate) = Utilities.calculateHighestThroughput(from, to, knownPaths)

      val bandwidthOfLatencyPath: Float = Utilities.calculateBandwidth(latency._1.toSeq).toKbps
      val throughputOfLatencyPath: Float = Utilities.calculateThroughput(latency._1.toSeq).toKbps
      val latencyOfBandwidthPath: Float = Utilities.calculateLatency(bandwidth._1.toSeq).toMillis
      val throughputOfBandwidthPath: Float = Utilities.calculateThroughput(bandwidth._1.toSeq).toKbps
      val latencyOfThroughputPath: Float = Utilities.calculateLatency(throughput._1.toSeq).toMillis
      val bandwidthOfThroughputPath: Float = Utilities.calculateBandwidth(throughput._1.toSeq).toKbps

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
}
