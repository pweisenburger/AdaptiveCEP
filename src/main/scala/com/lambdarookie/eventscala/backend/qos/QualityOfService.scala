package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.system.{Host, Operator}

/**
  * Created by monur.
  */
class QualityOfService(source: Host, value: Long) {
  def lower(limit: Long): Boolean = {
    value < limit
  }

  def higher(limit: Long): Boolean = {
    value > limit
  }

  def within(limit: Long): Boolean = {
    value <= limit
  }
}

case class Frequency(source: Host, frequency: Long) extends QualityOfService(source, frequency)

case class Latency(source: Host, destination: Host, latency: Long) extends QualityOfService(source, latency) {
//  def this(operator: Operator, latency: Latency) = ???
}

case class Proximity(source: Host, destination: Host, proximity: Long) extends QualityOfService(source, proximity)

case class Throughput(source: Host, destination: Host, throughput: Long) extends QualityOfService(source, throughput)

case class Bandwidth(broker: Host, bandwidth: Long) extends QualityOfService(broker, bandwidth)