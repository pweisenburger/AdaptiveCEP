package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.backend.qos.QoSUnits._
import rescala._

/**
  * Created by monur.
  */
sealed trait Host {
  val position: Coordinate

  def neighbors: Set[Host]
  def neighborLatencies: Map[Host, TimeSpan]
  def neighborBandwidths: Map[Host, BitRate]
  def neighborThroughputs: Map[Host, BitRate]
}



trait HostImpl extends Host {
  def measureLatencyToNeighbor(neighbor: Host): TimeSpan
  def measureBandwidthToNeighbor(neighbor: Host): BitRate
  def measureThroughputToNeighbor(neighbor: Host): BitRate


  var neighbors: Set[Host] = Set.empty
  var neighborLatencies: Map[Host, TimeSpan] = Map(this -> 0.ms)
  var neighborBandwidths: Map[Host, BitRate] = Map(this -> Int.MaxValue.gbps)
  var neighborThroughputs: Map[Host, BitRate] = Map(this -> Int.MaxValue.gbps)

  def measureNeighborLatencies(): Unit =
    neighbors.foreach { n => neighborLatencies += n -> measureLatencyToNeighbor(n) }

  def measureNeighborBandwidths(): Unit =
    neighbors.foreach { n => neighborBandwidths += n -> measureBandwidthToNeighbor(n) }

  def measureNeighborThroughputs(): Unit =
    neighbors.foreach { n => neighborThroughputs += n -> measureThroughputToNeighbor(n) }
}