package com.lambdarookie.eventscala.backend.data

import scala.concurrent.duration._

/**
  * Created by monur.
  */

object QoSUnits {

  trait QoSUnit {
    def <(value: QoSUnit): Boolean
    def >(value: QoSUnit): Boolean
  }



  case class TimeSpan(i: Duration) extends QoSUnit {
    override def <(value: QoSUnit): Boolean = {
      value match {
        case TimeSpan(duration) => i < duration
        case _ => throw new IllegalArgumentException
      }
    }
    override def >(value: QoSUnit): Boolean = {
      value match {
        case TimeSpan(duration) => i > duration
        case _ => throw new IllegalArgumentException
      }
    }
  }

  case class TimeSpanHelper(i: Int) {
    def ns: TimeSpan = TimeSpan(i.nanos)
    def ms: TimeSpan = TimeSpan(i.millis)
    def sec: TimeSpan = TimeSpan(i.second)
  }

  implicit def intToTimeSpanHelper(i: Int): TimeSpanHelper = TimeSpanHelper(i)



  trait Distance extends QoSUnit

  case class Meter(i: Int) extends Distance {
    override def <(value: QoSUnit): Boolean = {
      value match {
        case Meter(m) => i < m
        case Kilometer(km) => i < km * 1000
        case _ => throw new IllegalArgumentException
      }
    }
    override def >(value: QoSUnit): Boolean = {
      value match {
        case Meter(m) => i > m
        case Kilometer(km) => i > km * 1000
        case _ => throw new IllegalArgumentException
      }
    }
  }

  case class Kilometer(i: Int) extends Distance {
    override def <(value: QoSUnit): Boolean = {
      value match {
        case Meter(m) => i * 1000 < m
        case Kilometer(km) => i < km
        case _ => throw new IllegalArgumentException
      }
    }
    override def >(value: QoSUnit): Boolean = {
      value match {
        case Meter(m) => i * 1000 > m
        case Kilometer(km) => i > km
        case _ => throw new IllegalArgumentException
      }
    }
  }

  case class DistanceHelper(i: Int) {
    def m: Meter = Meter(i)
    def km: Kilometer = Kilometer(i)
  }

  implicit def intToDistanceHelper(i: Int): DistanceHelper = DistanceHelper(i)
}