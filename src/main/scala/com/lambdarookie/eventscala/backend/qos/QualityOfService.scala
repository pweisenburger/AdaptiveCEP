package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.system.traits.Operator

/**
  * Created by monur.
  */
object QualityOfService {
  sealed trait BooleanOperator
  case object Equal        extends BooleanOperator
  case object NotEqual     extends BooleanOperator
  case object Greater      extends BooleanOperator
  case object GreaterEqual extends BooleanOperator
  case object Smaller      extends BooleanOperator
  case object SmallerEqual extends BooleanOperator

  sealed trait Condition{ var notFulfilled: Boolean = false }
  case class FrequencyCondition(booleanOperator: BooleanOperator, ratio: Ratio) extends Condition
  case class ProximityCondition(booleanOperator: BooleanOperator, distance: Distance) extends Condition

  sealed trait Demand extends Condition { val conditions: Set[Condition] }
  case class LatencyDemand(booleanOperator: BooleanOperator, timeSpan: TimeSpan, conditions: Set[Condition])
    extends Demand
  case class ThroughputDemand(booleanOperator: BooleanOperator, bitRate: BitRate, conditions: Set[Condition])
    extends Demand
  case class BandwidthDemand(booleanOperator: BooleanOperator, bitRate: BitRate, conditions: Set[Condition])
    extends Demand


  def frequency: FrequencyConditionCreator.type = FrequencyConditionCreator
  case object FrequencyConditionCreator {
    def === (ratio: Ratio): FrequencyCondition = FrequencyCondition(Equal, ratio)
    def =!= (ratio: Ratio): FrequencyCondition = FrequencyCondition(NotEqual, ratio)
    def >   (ratio: Ratio): FrequencyCondition = FrequencyCondition(Greater, ratio)
    def >=  (ratio: Ratio): FrequencyCondition = FrequencyCondition(GreaterEqual, ratio)
    def <   (ratio: Ratio): FrequencyCondition = FrequencyCondition(Smaller, ratio)
    def <=  (ratio: Ratio): FrequencyCondition = FrequencyCondition(SmallerEqual, ratio)
  }

  def proximity: ProximityConditionCreator.type = ProximityConditionCreator
  case object ProximityConditionCreator {
    def === (distance: Distance): ProximityCondition = ProximityCondition(Equal, distance)
    def =!= (distance: Distance): ProximityCondition = ProximityCondition(NotEqual, distance)
    def >   (distance: Distance): ProximityCondition = ProximityCondition(Greater, distance)
    def >=  (distance: Distance): ProximityCondition = ProximityCondition(GreaterEqual, distance)
    def <   (distance: Distance): ProximityCondition = ProximityCondition(Smaller, distance)
    def <=  (distance: Distance): ProximityCondition = ProximityCondition(SmallerEqual, distance)
  }

  def latency: LatencyDemandCreator.type = LatencyDemandCreator
  case object LatencyDemandCreator {
    def === (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemand (Equal, timeSpan, conditions.toSet)
    def =!= (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemand (NotEqual, timeSpan, conditions.toSet)
    def >   (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemand (Greater, timeSpan, conditions.toSet)
    def >=  (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemand (GreaterEqual, timeSpan, conditions.toSet)
    def <   (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemand (Smaller, timeSpan, conditions.toSet)
    def <=  (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemand (SmallerEqual, timeSpan, conditions.toSet)
  }

  def throughput: ThroughputDemandCreator.type = ThroughputDemandCreator
  case object ThroughputDemandCreator {
    def === (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemand (Equal, bitRate, conditions.toSet)
    def =!= (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemand (NotEqual, bitRate, conditions.toSet)
    def >   (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemand (Greater, bitRate, conditions.toSet)
    def >=  (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemand (GreaterEqual, bitRate, conditions.toSet)
    def <   (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemand (Smaller, bitRate, conditions.toSet)
    def <=  (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemand (SmallerEqual, bitRate, conditions.toSet)
  }

  def bandwidth: BandwidthDemandCreator.type = BandwidthDemandCreator
  case object BandwidthDemandCreator {
    def === (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemand (Equal, bitRate, conditions.toSet)
    def =!= (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemand (NotEqual, bitRate, conditions.toSet)
    def >   (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemand (Greater, bitRate, conditions.toSet)
    def >=  (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemand (GreaterEqual, bitRate, conditions.toSet)
    def <   (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemand (Smaller, bitRate, conditions.toSet)
    def <=  (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemand (SmallerEqual, bitRate, conditions.toSet)
  }


  case class Violation(operator: Operator, demand: Demand)
}