package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.data.QoSUnits._

/**
  * Created by monur.
  */
trait Host {
  val position: Coordinate
  val maxBandwidth: BitRate

  def neighbors: Set[Host]
  def measureFrequency(): Unit
  def measureNeighborLatencies(): Unit
  def measureNeighborBandwidths(): Unit
  def measureNeighborThroughputs(): Unit

  var lastFrequency: Ratio = Ratio(0.instances, 0.sec)
  var lastProximities: Map[Host, Distance] = Map(this -> 0.m)
  var lastLatencies: Map[Host, TimeSpan] = Map(this -> 0.ms)
  var lastThroughputs: Map[Host, BitRate] = Map(this -> maxBandwidth)
  var lastBandwidths: Map[Host, BitRate] = Map(this -> maxBandwidth)


  def measureProximities(): Unit =
    neighbors.foreach(n => lastProximities += (n -> position.calculateDistanceTo(n.position).m))

  def sortByProximity(): Unit = lastProximities = Map(lastProximities.toSeq.sortWith(_._2 < _._2): _*)

  def measureMetrics(): Unit = {
    measureFrequency()
    measureProximities()
    measureNeighborLatencies()
    measureNeighborBandwidths()
    measureNeighborThroughputs()
  }
}
