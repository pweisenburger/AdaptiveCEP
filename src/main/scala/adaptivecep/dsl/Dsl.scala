package adaptivecep.dsl

import java.time.Duration

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import shapeless.ops.hlist.{HKernelAux, Prepend}
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

  def nStream[Types <: HList](publisherName: String)(implicit op: HKernelAux[Types]): HListNStream[Types] = HListNStream[Types](publisherName)(op)

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

  def stream[Types <: HList](publisherName: String, requirements: Requirement*)(implicit op: HKernelAux[Types]): HListQuery[Types] = Stream(publisherName, requirements.toSet)(op)

  case class SequenceHelper[ATypes <: HList](s: HListNStream[ATypes]) {
    def ->[BTypes <: HList](s2: HListNStream[BTypes]): (HListNStream[ATypes], HListNStream[BTypes]) = (s, s2)
  }

  implicit def nStreamToSequenceHelper[Types <: HList](s: HListNStream[Types]): SequenceHelper[Types] = SequenceHelper(s)

  def sequence[ATypes <: HList, BTypes <: HList, RTypes <: HList](tuple: (HListNStream[ATypes], HListNStream[BTypes]), requirements: Requirement*)(implicit p: Prepend.Aux[ATypes, BTypes, RTypes], op: HKernelAux[RTypes]):
    Sequence[ATypes, BTypes, RTypes] = Sequence(tuple._1, tuple._2, requirements.toSet)(p, op)

  implicit def queryToQueryHelper[ATypes <: HList](q: HListQuery[ATypes]): QueryHelper[ATypes] = QueryHelper(q)

  case class QueryHelper[ATypes <: HList](q: HListQuery[ATypes]) {
    def where(cond: ATypes => Boolean, requirements: Requirement*)(implicit op: HKernelAux[ATypes]): HListQuery[ATypes] = Filter(q, toFunEventBoolean(cond), requirements.toSet)(op)
    def selfJoin[RTypes <: HList](w1: Window, w2: Window, requirements: Requirement*)(implicit p: Prepend.Aux[ATypes, ATypes, RTypes], op: HKernelAux[RTypes]): HListQuery[p.Out] = SelfJoin[ATypes, p.Out](q, w1, w2,requirements.toSet)(p, op)
    def join[BTypes <: HList, RTypes <: HList](q2: HListQuery[BTypes], w1: Window, w2: Window, requirements: Requirement*)(implicit p: Prepend.Aux[ATypes, BTypes, RTypes], op: HKernelAux[RTypes]): HListQuery[p.Out] = Join[ATypes, BTypes, p.Out](q, q2, w1, w2, requirements.toSet)(p, op)
    def and[BTypes <: HList, RTypes <: HList](q2: HListQuery[BTypes], requirements: Requirement*)(implicit p: Prepend.Aux[ATypes, BTypes, RTypes], op: HKernelAux[RTypes]): HListQuery[RTypes] = Conjunction (q, q2, requirements.toSet)(p, op)
    // TODO i think i need another Query a CoproductQuery?
    // def or[BTypes <: HList](q2: HListQuery[BTypes], requirements: Requirement*): HListQuery[Either[ATypes, BTypes]] = Disjunction(q, q2, requirements.toSet)
  }
}
