package adaptivecep.dsl

import java.time.Duration
import adaptivecep.data.Events._
import adaptivecep.data.Queries._

import shapeless.{HList, HNil}

object Dsl {

  trait Timespan
  case class Nanoseconds(i: Int) extends Timespan
  case class Milliseconds(i: Int) extends Timespan
  case class Seconds(i: Int) extends Timespan

  case class TimespanHelper(i: Int) {
    def nanoseconds: Nanoseconds = Nanoseconds(i)
    def milliseconds: Milliseconds = Milliseconds(i)
    def seconds: Seconds = Seconds(i)
  }

  implicit def intToTimespanHelper(i: Int): TimespanHelper = TimespanHelper(i)

  case class Instances(i: Int)

  case class InstancesHelper(i: Int) {
    def instances: Instances = Instances(i)
  }

  implicit def intToInstancesHelper(i: Int): InstancesHelper = InstancesHelper(i)


  def slidingWindow  (instances: Instances): Window = SlidingInstances  (instances.i)
  def slidingWindow  (seconds: Seconds):     Window = SlidingTime       (seconds.i)
  def tumblingWindow (instances: Instances): Window = TumblingInstances (instances.i)
  def tumblingWindow (seconds: Seconds):     Window = TumblingTime      (seconds.i)

  def nStream[Types <: HList](publisherName: String): NStream = HListNStream[Types](publisherName)

  case class Ratio(instances: Instances, seconds: Seconds)

  def ratio(instances: Instances, seconds: Seconds): Ratio = Ratio(instances, seconds)

  def frequency: FrequencyHelper.type = FrequencyHelper

  case object FrequencyHelper {
    def === (ratio: Ratio): FrequencyHelper2 = FrequencyHelper2(Equal, ratio)
    def =!= (ratio: Ratio): FrequencyHelper2 = FrequencyHelper2(NotEqual, ratio)
    def >   (ratio: Ratio): FrequencyHelper2 = FrequencyHelper2(Greater, ratio)
    def >=  (ratio: Ratio): FrequencyHelper2 = FrequencyHelper2(GreaterEqual, ratio)
    def <   (ratio: Ratio): FrequencyHelper2 = FrequencyHelper2(Smaller, ratio)
    def <=  (ratio: Ratio): FrequencyHelper2 = FrequencyHelper2(SmallerEqual, ratio)
  }

  case class FrequencyHelper2(operator: Operator, ratio: Ratio) {
    def otherwise(callback: NodeData => Any): FrequencyRequirement =
      FrequencyRequirement(operator, ratio.instances.i, ratio.seconds.i, callback)
  }

  def timespan(timespan: Timespan): Duration = timespan match {
    case Nanoseconds(i) => Duration.ofNanos(i)
    case Milliseconds(i) => Duration.ofMillis(i)
    case Seconds(i) => Duration.ofSeconds(i)
  }

  def latency: LatencyHelper.type = LatencyHelper

  case object LatencyHelper {
    def === (duration: Duration): LatencyHelper2 = LatencyHelper2 (Equal, duration)
    def =!= (duration: Duration): LatencyHelper2 = LatencyHelper2 (NotEqual, duration)
    def >   (duration: Duration): LatencyHelper2 = LatencyHelper2 (Greater, duration)
    def >=  (duration: Duration): LatencyHelper2 = LatencyHelper2 (GreaterEqual, duration)
    def <   (duration: Duration): LatencyHelper2 = LatencyHelper2 (Smaller, duration)
    def <=  (duration: Duration): LatencyHelper2 = LatencyHelper2 (SmallerEqual, duration)
  }

  case class LatencyHelper2(operator: Operator, duration: Duration) {
    def otherwise(callback: NodeData => Any): LatencyRequirement =
      LatencyRequirement(operator, duration, callback)
  }

  def stream[Types <: HList](publisherName: String, requirements: Requirement*): HListQuery[Types] = Stream(publisherName, requirements.toSet)

  case class SequenceHelper[TypesA <: HList](s: HListNStream[TypesA]) {
    def ->[TypesB <: HList](s2: HListNStream[TypesB]): (HListNStream[TypesA], HListNStream[TypesB]) = (s, s2)
  }

  implicit def nStreamToSequenceHelper[Types <: HList](s: HListNStream[Types]): SequenceHelper[Types] = SequenceHelper(s)

  // TODO type level concatenation of HList
  def sequence[ATypes <: HList, BTypes <: HList, RTypes <: HList](tuple: (HListNStream[ATypes], HListNStream[BTypes]), requirements: Requirement*):
    Sequence[ATypes, BTypes, RTypes] = Sequence(tuple._1, tuple._2, requirements.toSet)

  case class QueryHelper[ATypes <: HList](q: HListQuery[ATypes]) {
    def where(cond: (ATypes) => Boolean, requirements: Requirement*): HListQuery[ATypes] = Filter1(q, toFunEventBoolean(cond), requirements.toSet)
    def selfJoin[RTypes <: HList](w1: Window, w2: Window, requirements: Requirement*): HListQuery[RTypes] = SelfJoin(q, w1, w2,requirements.toSet)
    def join[BTypes <: HList, RTypes <: HList](q2: HListQuery[BTypes],w1: Window, w2: Window, requirements: Requirement*): HListQuery[RTypes] = Join(q, q2, w1, w2, requirements.toSet)
    def and[BTypes <: HList, RTypes <: HList](q2: HListQuery[BTypes], requirements: Requirement*): HListQuery[RTypes] = Conjunction (q, q2, requirements.toSet)
    // TODO i think i need another Query a CoproductQuery?
    // def or[BTypes <: HList](q2: HListQuery[BTypes], requirements: Requirement*): HListQuery[Either[ATypes, BTypes]] = Disjunction(q, q2, requirements.toSet)
  }


  implicit def queryToQueryHelper[ATypes <: HList]                (q: HListQuery[ATypes]): QueryHelper[ATypes] = QueryHelper(q)
}
