package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import rescala._

/**
  * Created by monur.
  */
trait Host {
  val position: Coordinate

  def neighbors: Set[Host]
  def measureLatencyToNeighbor(neighbor: Host): TimeSpan
  def measureBandwidthToNeighbor(neighbor: Host): BitRate
  def measureThroughputToNeighbor(neighbor: Host): BitRate


  private val operatorsVar: Var[Set[Operator]] = Var(Set.empty)

  val operators: Signal[Set[Operator]] = operatorsVar

  var neighborLatencies: Map[Host, TimeSpan] = Map(this -> 0.ns)
  var neighborBandwidths: Map[Host, BitRate] = Map(this -> Int.MaxValue.gbps)
  var neighborThroughputs: Map[Host, BitRate] = Map(this -> Int.MaxValue.gbps)

  def measureNeighborLatencies(): Unit =
    neighbors.foreach { n => neighborLatencies += n -> measureLatencyToNeighbor(n) }

  def measureNeighborBandwidths(): Unit =
    neighbors.foreach { n => neighborBandwidths += n -> measureBandwidthToNeighbor(n) }

  def measureNeighborThroughputs(): Unit =
    neighbors.foreach { n => neighborThroughputs += n -> measureThroughputToNeighbor(n) }

  def measureMetrics(): Unit = {
    measureNeighborLatencies()
    measureNeighborBandwidths()
    measureNeighborThroughputs()
  }

  def addOperator(operator: Operator): Unit = operatorsVar.transform(_ + operator)

  def removeOperator(operator: Operator): Unit = operatorsVar.transform(_ - operator)
}
