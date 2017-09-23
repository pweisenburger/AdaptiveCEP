package com.lambdarookie.eventscala.backend.data

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


  //          TimeSpan Begin
  case class TimeSpan(duration: Duration) extends QoSUnit[TimeSpan] {
    override def <(other: TimeSpan): Boolean = duration.compareTo(other.duration) < 0
    override def >(other: TimeSpan): Boolean = duration.compareTo(other.duration) > 0
    override def <=(other: TimeSpan): Boolean = duration.compareTo(other.duration) <= 0
    override def -(other: TimeSpan): TimeSpan = TimeSpan(duration minus other.duration)
    def +(other: TimeSpan): TimeSpan = TimeSpan(duration plus other.duration)

    def toNanos: Long = duration.toNanos
    def toMillis: Int = duration.toMillis.toInt
    def toSeconds: Int = duration.getSeconds.toInt


    override def toString: String = if (toSeconds > 0)
      s"$toSeconds.s"
    else if (toMillis > 0)
      s"$toMillis.ms"
    else
      s"$toNanos.ns"
  }

  case class TimeSpanUnits(private val i: Int) {
    def ns: TimeSpan = TimeSpan(Duration.ofNanos(i))
    def ms: TimeSpan = TimeSpan(Duration.ofMillis(i))
    def sec: TimeSpan = TimeSpan(Duration.ofSeconds(i))
  }

  implicit def intToTimeSpanCreator(i: Int): TimeSpanUnits = TimeSpanUnits(i)
  //          TimeSpan End


  //          Distance Begin
  trait Distance extends QoSUnit[Distance] {
    def toMeter: Int

    override def <(other: Distance): Boolean = this.toMeter < other.toMeter
    override def >(other: Distance): Boolean = this.toMeter > other.toMeter
    override def <=(other: Distance): Boolean = this.toMeter <= other.toMeter

    override def toString: String = this match {
      case Meter(m) => s"$m.m"
      case Kilometer(km) => s"$km.km"
    }
  }

  case class Meter(i: Int) extends Distance {
    override def toMeter: Int = i
    override def -(other: Distance): Distance = Meter(this.toMeter - other.toMeter)
  }

  case class Kilometer(i: Int) extends Distance {
    override def toMeter: Int = i * 1000
    override def -(other: Distance): Distance = Kilometer((this.toMeter - other.toMeter)/1000)
  }

  case class DistanceUnits(i: Int) {
    def m: Meter = Meter(i)
    def km: Kilometer = Kilometer(i)
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
    val exactRatio: Double = instances.getInstanceNum.toDouble / timeSpan.toNanos.toDouble

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