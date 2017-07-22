package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.system.traits.Host

import scala.collection.SortedSet

/**
  * Created by monur.
  */
sealed trait QoSMetric
sealed trait Conditionable extends QoSMetric
sealed trait Demandable extends Conditionable

case class Proximity(source: Host, destination: Host, proximity: Distance) extends Conditionable {
  def nearest(count: Int): SortedSet[Host] = {
    source.sortNeighborsByProximity.grouped(count).toList(1)
  }
}

case class Frequency(source: Host, ratio: Ratio) extends Conditionable

case class Latency(source: Host, destination: Host, latency: TimeSpan) extends Demandable

case class Throughput(source: Host, destination: Host, throughput: BitRate) extends Demandable

case class Bandwidth(broker: Host, bandwidth: BitRate) extends Demandable