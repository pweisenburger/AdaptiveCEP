package com.lambdarookie.eventscala.backend.qos

import QoSUnits._
import com.lambdarookie.eventscala.backend.system.traits.{Host, Operator}

/**
  * Created by monur.
  */
object QualityOfService {
  sealed trait BooleanOperator
  case object Equal        extends BooleanOperator { override def toString: String = "==" }
  case object NotEqual     extends BooleanOperator { override def toString: String = "!=" }
  case object Greater      extends BooleanOperator { override def toString: String = ">"  }
  case object GreaterEqual extends BooleanOperator { override def toString: String = ">=" }
  case object Smaller      extends BooleanOperator { override def toString: String = "<"  }
  case object SmallerEqual extends BooleanOperator { override def toString: String = "<=" }



  sealed trait Condition{
    val booleanOperator: BooleanOperator

    def notFulfilled: Boolean

    override def toString: String = this match {
      case fc: FrequencyCondition => s"Frequency ${fc.booleanOperator} ${fc.ratio}"
      case pc: ProximityCondition => s"Proximity ${pc.booleanOperator} ${pc.distance}"
      case lc: LatencyDemand => s"Latency ${lc.booleanOperator} ${lc.timeSpan}"
      case bd: BandwidthDemand => s"Bandwidth ${bd.booleanOperator} ${bd.bitRate}"
      case td: ThroughputDemand => s"Throughput ${td.booleanOperator} ${td.bitRate}"
    }
  }

  trait FrequencyCondition extends Condition { val ratio: Ratio }
  trait ProximityCondition extends Condition { val distance: Distance }


  sealed trait Demand extends Condition {
    val conditions: Set[Condition]

    override def toString: String = super.toString + (if (conditions.nonEmpty) s" with conditions: $conditions" else "")
  }

  trait LatencyDemand extends Demand { val timeSpan: TimeSpan }
  trait BandwidthDemand extends Demand { val bitRate: BitRate }
  trait ThroughputDemand extends Demand { val bitRate: BitRate }


  private[backend] sealed trait ConditionImpl extends  Condition { var notFulfilled: Boolean = true }

  private[backend] case class FrequencyConditionImpl(booleanOperator: BooleanOperator, ratio: Ratio)
    extends FrequencyCondition with ConditionImpl
  private[backend] case class ProximityConditionImpl(booleanOperator: BooleanOperator, distance: Distance)
    extends ProximityCondition with ConditionImpl

  private[backend] case class LatencyDemandImpl(booleanOperator: BooleanOperator, timeSpan: TimeSpan, conditions: Set[Condition])
    extends LatencyDemand with ConditionImpl
  private[backend] case class BandwidthDemandImpl(booleanOperator: BooleanOperator, bitRate: BitRate, conditions: Set[Condition])
    extends BandwidthDemand with ConditionImpl
  private[backend] case class ThroughputDemandImpl(booleanOperator: BooleanOperator, bitRate: BitRate, conditions: Set[Condition])
    extends ThroughputDemand with ConditionImpl


  case class Violation(operator: Operator, demand: Demand)

  case class Adaptation(assignments: Map[Operator, Host])



  def frequency: FrequencyConditionCreator.type = FrequencyConditionCreator
  case object FrequencyConditionCreator {
    def === (ratio: Ratio): FrequencyCondition = FrequencyConditionImpl(Equal, ratio)
    def =!= (ratio: Ratio): FrequencyCondition = FrequencyConditionImpl(NotEqual, ratio)
    def >   (ratio: Ratio): FrequencyCondition = FrequencyConditionImpl(Greater, ratio)
    def >=  (ratio: Ratio): FrequencyCondition = FrequencyConditionImpl(GreaterEqual, ratio)
    def <   (ratio: Ratio): FrequencyCondition = FrequencyConditionImpl(Smaller, ratio)
    def <=  (ratio: Ratio): FrequencyCondition = FrequencyConditionImpl(SmallerEqual, ratio)
  }

  def proximity: ProximityConditionCreator.type = ProximityConditionCreator
  case object ProximityConditionCreator {
    def === (distance: Distance): ProximityCondition = ProximityConditionImpl(Equal, distance)
    def =!= (distance: Distance): ProximityCondition = ProximityConditionImpl(NotEqual, distance)
    def >   (distance: Distance): ProximityCondition = ProximityConditionImpl(Greater, distance)
    def >=  (distance: Distance): ProximityCondition = ProximityConditionImpl(GreaterEqual, distance)
    def <   (distance: Distance): ProximityCondition = ProximityConditionImpl(Smaller, distance)
    def <=  (distance: Distance): ProximityCondition = ProximityConditionImpl(SmallerEqual, distance)
  }

  def latency: LatencyDemandCreator.type = LatencyDemandCreator
  case object LatencyDemandCreator {
    def === (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemandImpl (Equal, timeSpan, conditions.toSet)
    def =!= (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemandImpl (NotEqual, timeSpan, conditions.toSet)
    def >   (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemandImpl (Greater, timeSpan, conditions.toSet)
    def >=  (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemandImpl (GreaterEqual, timeSpan, conditions.toSet)
    def <   (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemandImpl (Smaller, timeSpan, conditions.toSet)
    def <=  (timeSpan: TimeSpan, conditions: Condition*): LatencyDemand =
      LatencyDemandImpl (SmallerEqual, timeSpan, conditions.toSet)
  }

  def bandwidth: BandwidthDemandCreator.type = BandwidthDemandCreator
  case object BandwidthDemandCreator {
    def === (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemandImpl (Equal, bitRate, conditions.toSet)
    def =!= (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemandImpl (NotEqual, bitRate, conditions.toSet)
    def >   (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemandImpl (Greater, bitRate, conditions.toSet)
    def >=  (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemandImpl (GreaterEqual, bitRate, conditions.toSet)
    def <   (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemandImpl (Smaller, bitRate, conditions.toSet)
    def <=  (bitRate: BitRate, conditions: Condition*): BandwidthDemand =
      BandwidthDemandImpl (SmallerEqual, bitRate, conditions.toSet)
  }

  def throughput: ThroughputDemandCreator.type = ThroughputDemandCreator
  case object ThroughputDemandCreator {
    def === (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemandImpl (Equal, bitRate, conditions.toSet)
    def =!= (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemandImpl (NotEqual, bitRate, conditions.toSet)
    def >   (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemandImpl (Greater, bitRate, conditions.toSet)
    def >=  (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemandImpl (GreaterEqual, bitRate, conditions.toSet)
    def <   (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemandImpl (Smaller, bitRate, conditions.toSet)
    def <=  (bitRate: BitRate, conditions: Condition*): ThroughputDemand =
      ThroughputDemandImpl (SmallerEqual, bitRate, conditions.toSet)
  }

  def isFulfilled[T <: QoSUnit[T]](value: QoSUnit[T], condition: Condition): Boolean = condition match {
    case fc: FrequencyCondition =>
      if (!value.isInstanceOf[Ratio]) throw new IllegalArgumentException("QoS metric does not match the condition.")
      val frequency: Ratio = value.asInstanceOf[Ratio]
      fc.booleanOperator match {
        case Equal =>        frequency == fc.ratio
        case NotEqual =>     frequency != fc.ratio
        case Greater =>      frequency > fc.ratio
        case GreaterEqual => frequency >= fc.ratio
        case Smaller =>      frequency < fc.ratio
        case SmallerEqual => frequency <= fc.ratio
      }
    case pc: ProximityCondition =>
      if (!value.isInstanceOf[Distance]) throw new IllegalArgumentException("QoS metric does not match the condition.")
      val proximity: Distance = value.asInstanceOf[Distance]
      pc.booleanOperator match {
        case Equal =>        proximity == pc.distance
        case NotEqual =>     proximity != pc.distance
        case Greater =>      proximity > pc.distance
        case GreaterEqual => proximity >= pc.distance
        case Smaller =>      proximity < pc.distance
        case SmallerEqual => proximity <= pc.distance
      }
    case lc: LatencyDemand =>
      if (!value.isInstanceOf[TimeSpan]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val latency: TimeSpan = value.asInstanceOf[TimeSpan]
      lc.booleanOperator match {
        case Equal =>        latency == lc.timeSpan
        case NotEqual =>     latency != lc.timeSpan
        case Greater =>      latency > lc.timeSpan
        case GreaterEqual => latency >= lc.timeSpan
        case Smaller =>      latency < lc.timeSpan
        case SmallerEqual => latency <= lc.timeSpan
      }
    case bd: BandwidthDemand =>
      if (!value.isInstanceOf[BitRate]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val bandwidth: BitRate = value.asInstanceOf[BitRate]
      bd.booleanOperator match {
        case Equal =>        bandwidth == bd.bitRate
        case NotEqual =>     bandwidth != bd.bitRate
        case Greater =>      bandwidth > bd.bitRate
        case GreaterEqual => bandwidth >= bd.bitRate
        case Smaller =>      bandwidth < bd.bitRate
        case SmallerEqual => bandwidth <= bd.bitRate
      }
    case td: ThroughputDemand =>
      if (!value.isInstanceOf[BitRate]) throw new IllegalArgumentException("QoS metric does not match the demand.")
      val throughput: BitRate = value.asInstanceOf[BitRate]
      td.booleanOperator match {
        case Equal =>        throughput == td.bitRate
        case NotEqual =>     throughput != td.bitRate
        case Greater =>      throughput > td.bitRate
        case GreaterEqual => throughput >= td.bitRate
        case Smaller =>      throughput < td.bitRate
        case SmallerEqual => throughput <= td.bitRate
      }
  }
}