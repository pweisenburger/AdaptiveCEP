package com.lambdarookie.eventscala.backend.data

import scala.concurrent.duration._

/**
  * Created by monur.
  */

object QoSUnits {

  trait QoSUnit[T <: QoSUnit[T]] {
    def <(value: T): Boolean
    def >(value: T): Boolean
    def <=(value: T): Boolean

    def -(value: T): T
  }



  case class TimeSpan(i: Duration) extends QoSUnit[TimeSpan] {
    override def <(value: TimeSpan): Boolean = value match {case TimeSpan(duration) => i < duration}
    override def >(value: TimeSpan): Boolean = value match {case TimeSpan(duration) => i > duration}
    override def <=(value: TimeSpan): Boolean = value match {case TimeSpan(duration) => i <= duration}
    override def -(value: TimeSpan): TimeSpan = value match {case TimeSpan(duration) => TimeSpan(i - duration)}
  }

  case class TimeSpanUnits(i: Int) {
    def ns: TimeSpan = TimeSpan(i.nanos)
    def ms: TimeSpan = TimeSpan(i.millis)
    def sec: TimeSpan = TimeSpan(i.second)
  }

  implicit def intToTimeSpanCreator(i: Int): TimeSpanUnits = TimeSpanUnits(i)



  trait Distance extends QoSUnit[Distance] {
    def toMeter: Int
    override def <(value: Distance): Boolean = this.toMeter < value.toMeter
    override def >(value: Distance): Boolean = this.toMeter > value.toMeter
    override def <=(value: Distance): Boolean = this.toMeter <= value.toMeter
  }

  case class Meter(i: Int) extends Distance {
    override def toMeter: Int = i
    override def -(value: Distance): Distance = Meter(this.toMeter - value.toMeter)
  }

  case class Kilometer(i: Int) extends Distance {
    override def toMeter: Int = i * 1000
    override def -(value: Distance): Distance = Kilometer((this.toMeter - value.toMeter)/1000)
  }

  case class DistanceUnits(i: Int) {
    def m: Meter = Meter(i)
    def km: Kilometer = Kilometer(i)
  }

  implicit def intToDistanceCreator(i: Int): DistanceUnits = DistanceUnits(i)


  trait FrequencyUnit extends QoSUnit[FrequencyUnit]



  trait BitRate extends QoSUnit[BitRate] {
    def toKbps: Long
    override def <(value: BitRate): Boolean = this.toKbps < value.toKbps
    override def >(value: BitRate): Boolean = this.toKbps > value.toKbps
    override def <=(value: BitRate): Boolean = this.toKbps <= value.toKbps
  }

  case class KilobitsPerSecond(i: Long) extends BitRate {
    override def toKbps: Long = i
    override def -(value: BitRate): BitRate = KilobitsPerSecond(this.toKbps - value.toKbps)
  }

  case class MegabitsPerSecond(i: Long) extends BitRate {
    override def toKbps: Long = i * 1024
    override def -(value: BitRate): BitRate = MegabitsPerSecond((this.toKbps - value.toKbps)/1024)
  }

  case class GigabitsPerSecond(i: Long) extends BitRate {
    override def toKbps: Long = i * 1024 * 1024
    override def -(value: BitRate): BitRate = GigabitsPerSecond((this.toKbps - value.toKbps)/1024/1024)
  }

  case class BitRateUnits(i: Long) {
    def kbps: KilobitsPerSecond = KilobitsPerSecond(i)
    def mbps: MegabitsPerSecond = MegabitsPerSecond(i)
    def gbps: GigabitsPerSecond = GigabitsPerSecond(i)
  }

  implicit def longToBitRateCreator(i: Long): BitRateUnits = BitRateUnits(i)
}