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

  sealed trait Requirement
  case class LatencyRequirement   (booleanOperator: BooleanOperator, timeSpan: TimeSpan) extends Requirement
  case class FrequencyRequirement (booleanOperator: BooleanOperator, ratio: Ratio) extends Requirement


  def frequency: FrequencyRequirementCreator.type = FrequencyRequirementCreator
  case object FrequencyRequirementCreator {
    def === (ratio: Ratio): FrequencyRequirement = FrequencyRequirement(Equal, ratio)
    def =!= (ratio: Ratio): FrequencyRequirement = FrequencyRequirement(NotEqual, ratio)
    def >   (ratio: Ratio): FrequencyRequirement = FrequencyRequirement(Greater, ratio)
    def >=  (ratio: Ratio): FrequencyRequirement = FrequencyRequirement(GreaterEqual, ratio)
    def <   (ratio: Ratio): FrequencyRequirement = FrequencyRequirement(Smaller, ratio)
    def <=  (ratio: Ratio): FrequencyRequirement = FrequencyRequirement(SmallerEqual, ratio)
  }

  def latency: LatencyRequirementCreator.type = LatencyRequirementCreator
  case object LatencyRequirementCreator {
    def === (timeSpan: TimeSpan): LatencyRequirement = LatencyRequirement (Equal, timeSpan)
    def =!= (timeSpan: TimeSpan): LatencyRequirement = LatencyRequirement (NotEqual, timeSpan)
    def >   (timeSpan: TimeSpan): LatencyRequirement = LatencyRequirement (Greater, timeSpan)
    def >=  (timeSpan: TimeSpan): LatencyRequirement = LatencyRequirement (GreaterEqual, timeSpan)
    def <   (timeSpan: TimeSpan): LatencyRequirement = LatencyRequirement (Smaller, timeSpan)
    def <=  (timeSpan: TimeSpan): LatencyRequirement = LatencyRequirement (SmallerEqual, timeSpan)
  }


  case class Demand(operator: Operator, requirement: Requirement)
}