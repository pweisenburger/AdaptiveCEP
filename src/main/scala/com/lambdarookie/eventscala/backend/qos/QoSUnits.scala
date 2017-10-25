package com.lambdarookie.eventscala.backend.qos

import java.time._

/**
  * Created by monur.
  */

object QoSUnits {

  trait QoSUnit[T <: QoSUnit[T]] {
    def <(other: T): Boolean
    def <=(other: T): Boolean
    def -(other: T): T

    def >(other: T): Boolean = other < this.asInstanceOf[T]
    def >=(other: T): Boolean = other <= this.asInstanceOf[T]
  }

  def min[T <: QoSUnit[T]](first: T, second: T): T = if(first < second) first else second
  def max[T <: QoSUnit[T]](first: T, second: T): T = if(first > second) first else second


  //          TimeSpan Begin
  case class TimeSpan(private val micros: Float) extends QoSUnit[TimeSpan] {
    override def <(other: TimeSpan): Boolean = micros < other.toMicros
    override def >(other: TimeSpan): Boolean = micros > other.toMicros
    override def <=(other: TimeSpan): Boolean = micros <= other.toMicros
    override def -(other: TimeSpan): TimeSpan = TimeSpan(micros - other.toMicros)
    def +(other: TimeSpan): TimeSpan = TimeSpan(micros + other.toMicros)

    def toMicros: Float = micros
    def toMillis: Float = micros / 1000f
    def toSeconds: Float = micros / 1000000f
    def toDuration: Duration = Duration.ofMillis(toMillis.toInt)


    override def toString: String = if (toSeconds > 1)
      s"$toSeconds s"
    else if (toMillis > 1)
      s"$toMillis ms"
    else
      s"$toMicros μs"
  }

  case class TimeSpanUnits(private val i: Float) {
    def micros: TimeSpan = TimeSpan(i)
    def ms: TimeSpan = TimeSpan(i * 1000f)
    def sec: TimeSpan = TimeSpan(i * 1000000f)
  }

  implicit def floatToTimeSpanCreator(i: Float): TimeSpanUnits = TimeSpanUnits(i)
  //          TimeSpan End


  //          Distance Begin
  case class Distance(private val meters: Int) extends QoSUnit[Distance] {
    def toMeter: Int = meters
    def toKm: Int = meters / 1000

    override def <(other: Distance): Boolean = this.toMeter < other.toMeter
    override def >(other: Distance): Boolean = this.toMeter > other.toMeter
    override def <=(other: Distance): Boolean = this.toMeter <= other.toMeter
    override def -(other: Distance): Distance = Distance(meters - other.toMeter)

    override def toString: String = if (toKm > 0) s"$toKm.km" else s"$meters.m"
  }

  case class DistanceUnits(i: Int) {
    def m: Distance = Distance(i)
    def km: Distance = Distance(i * 1000)
  }

  implicit def intToDistanceCreator(i: Int): DistanceUnits = DistanceUnits(i)
  //          Distance End


  //          Ratio Begin
  case class Instances(private val i: Int) {
    def -(other: Instances): Instances = (this.getInstanceNum - other.getInstanceNum).instances
    def instances: Instances = Instances(i)
    def getInstanceNum: Int = i

    override def toString: String = s"$i.events"
  }
  implicit def intToInstancesCreator(i: Int): Instances = Instances(i)

  case class Ratio(instances: Instances, timeSpan: TimeSpan) extends QoSUnit[Ratio] {
    val exactRatio: Float = instances.getInstanceNum.toFloat / timeSpan.toMillis

    override def <(other: Ratio): Boolean = this.exactRatio < other.exactRatio
    override def >(other: Ratio): Boolean = this.exactRatio > other.exactRatio
    override def <=(other: Ratio): Boolean = this.exactRatio <= other.exactRatio
    override def -(other: Ratio): Ratio = Ratio(this.instances - other.instances, this.timeSpan - other.timeSpan)

    override def toString: String = s"$instances per $timeSpan"
  }
  //          Ratio End


  //          BitRate Begin
  case class BitRate(private val kbps: Long) extends QoSUnit[BitRate] {
    def toKbps: Long = kbps
    def toMbps: Long = kbps / 1024
    def toGbps: Long = toMbps / 1024

    override def <(other: BitRate): Boolean = this.toKbps < other.toKbps
    override def >(other: BitRate): Boolean = this.toKbps > other.toKbps
    override def <=(other: BitRate): Boolean = this.toKbps <= other.toKbps
    override def -(other: BitRate): BitRate = BitRate(kbps - other.toKbps)

    override def toString: String = if (toGbps > 0)
      s"$toGbps.gbps"
    else if (toMbps > 0)
      s"$toMbps.mbps"
    else
      s"$kbps.kbps"
  }

  case class BitRateUnits(private val i: Long) {
    def kbps: BitRate = BitRate(i)
    def mbps: BitRate = BitRate(i * 1024)
    def gbps: BitRate = BitRate(i * 1024 * 1024)
  }

  implicit def longToBitRateCreator(i: Long): BitRateUnits = BitRateUnits(i)
  //          BitRate End


  //          Coordinate Begin
  /**
    * @param latitude Latitude
    * @param longitude Longitude
    * @param altitude Altitude in meters
    */
  case class Coordinate(latitude: Double, longitude: Double, altitude: Double)
  //          Coordinate End
}