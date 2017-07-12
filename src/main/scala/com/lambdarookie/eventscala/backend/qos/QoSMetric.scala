package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.system.traits.Host

import scala.collection.SortedSet

/**
  * Created by monur.
  */
trait QoSMetric
class Conditionable[T <: QoSUnit[T]](host: Host, value: T) extends QoSMetric{
  def within(limit: T): Condition = {
    new Condition(value <= limit)
  }
  def higher(limit: T): Condition = {
    new Condition(value > limit)
  }
}
class Demandable[T <: QoSUnit[T]](host: Host, value: T) extends Conditionable(host, value) {
  def lower(limit: T): Demand = {
    new Demand(value < limit)
  }
  override def higher(limit: T): Demand = {
    new Demand(value > limit)
  }
}

case class Proximity(source: Host, destination: Host, proximity: Distance)
  extends Conditionable[Distance](source,  proximity) {

  def nearest(count: Int): SortedSet[Host] = {
    source.sortNeighborsByProximity.grouped(count).toList(1)
  }
}
case class Frequency(source: Host, ratio: Ratio) extends Conditionable[Ratio](source, ratio)

case class Latency(source: Host, destination: Host, latency: TimeSpan) extends Demandable[TimeSpan](source, latency)
case class Throughput(source: Host, destination: Host, throughput: BitRate) extends Demandable[BitRate](source,
  throughput)
case class Bandwidth(broker: Host, bandwidth: BitRate) extends Demandable[BitRate](broker, bandwidth)