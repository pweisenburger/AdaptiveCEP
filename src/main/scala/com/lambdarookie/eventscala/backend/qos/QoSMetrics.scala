package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.data.QoSUnits.{BitRate, TimeSpan}
import com.lambdarookie.eventscala.backend.system.Utilities
import com.lambdarookie.eventscala.backend.system.traits.Host

/**
  * Created by monur.
  */
object QoSMetrics {
  trait QoSMetric { val source: Host; val destination: Host; val hops: Seq[Host] }

  case class Latency(source: Host, destination: Host, hops: Seq[Host]) extends QoSMetric {
    def latency: TimeSpan = if (hops.isEmpty)
      source.neighborLatencies(destination)
    else
      Utilities.calculateLatency(source +: hops :+ destination)
  }

  case class Bandwidth(source: Host, destination: Host, hops: Seq[Host]) extends QoSMetric {
    def bandwidth: BitRate = if (hops.isEmpty)
      source.neighborBandwidths(destination)
    else
      Utilities.calculateBandwidth(source +: hops :+ destination)
  }

  case class Throughput(source: Host, destination: Host, hops: Seq[Host]) extends QoSMetric {
    def throughput: BitRate = if (hops.isEmpty)
      source.neighborThroughputs(destination)
    else
      Utilities.calculateThroughput(source +: hops :+ destination)
  }

  case class Priority(latencyWeight: Int, bandwidthWeight: Int, throughputWeight: Int) {
    def choosePath(from: Host, to: Host): QoSMetric = {
      def weightRate(weight: Float): Float = weight / (latencyWeight + bandwidthWeight + throughputWeight)

      val latency: (Latency, TimeSpan) = Utilities.calculateLowestLatency(from, to)
      val bandwidth: (Bandwidth, BitRate) = Utilities.calculateHighestBandwidth(from, to)
      val throughput: (Throughput, BitRate) = Utilities.calculateHighestThroughput(from, to)

      val bandwidthOfLatencyPath: Float = Utilities.calculateBandwidth(from +: latency._1.hops :+ to).toKbps
      val throughputOfLatencyPath: Float = Utilities.calculateThroughput(from +: latency._1.hops :+ to).toKbps
      val latencyOfBandwidthPath: Float = Utilities.calculateLatency(from +: bandwidth._1.hops :+ to).toMillis
      val throughputOfBandwidthPath: Float = Utilities.calculateThroughput(from +: bandwidth._1.hops :+ to).toKbps
      val latencyOfThroughputPath: Float = Utilities.calculateLatency(from +: throughput._1.hops :+ to).toMillis
      val bandwidthOfThroughputPath: Float = Utilities.calculateBandwidth(from +: throughput._1.hops :+ to).toKbps

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
