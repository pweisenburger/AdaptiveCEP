package adaptivecep.dsl

import java.time.Duration

import adaptivecep.data.{Disjunct, DropAt}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import shapeless.{::, HList, HNil, Nat}
import shapeless.ops.hlist.{HKernelAux, Prepend}
import shapeless.ops.nat.ToInt

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

  def nStream[T <: HList]
    (publisherName: String)
    (implicit op: HKernelAux[T]): HListNStream[T] = HListNStream[T](publisherName)(op)

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

  def stream[T <: HList](publisherName: String, requirements: Requirement*)
                        (implicit op: HKernelAux[T]): HListQuery[T] =
    Stream(publisherName, requirements.toSet)(op)

  case class SequenceHelper[A <: HList](s: HListNStream[A]) {
    def ->[B <: HList](s2: HListNStream[B]): (HListNStream[A], HListNStream[B]) = (s, s2)
  }

  implicit def nStreamToSequenceHelper[T <: HList](s: HListNStream[T]): SequenceHelper[T] = SequenceHelper(s)

  def sequence[A <: HList, B <: HList, R <: HList](tuple: (HListNStream[A], HListNStream[B]), requirements: Requirement*)
                                                  (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]): Sequence[A, B, R] =
      Sequence(tuple._1, tuple._2, requirements.toSet)(p, op)

  implicit def queryToQueryHelper[A <: HList](q: HListQuery[A]): QueryHelper[A] = QueryHelper(q)

  case class QueryHelper[A <: HList](q: HListQuery[A]) {
    def selfJoin[R <: HList](w1: Window, w2: Window, requirements: Requirement*)
                            (implicit p: Prepend.Aux[A, A, R], op: HKernelAux[R]): HListQuery[R] =
        SelfJoin[A, R](q, w1, w2, requirements.toSet)(p, op)
    def join[B <: HList, R <: HList](q2: HListQuery[B], w1: Window, w2: Window, requirements: Requirement*)
                                    (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]): HListQuery[R] =
        Join[A, B, R](q, q2, w1, w2, requirements.toSet)(p, op)
    def and[B <: HList, R <: HList](q2: HListQuery[B], requirements: Requirement*)
                                   (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]): HListQuery[R] =
        Conjunction(q, q2, requirements.toSet)(p, op)
    def drop[R <: HList, Pos <: Nat](pos: Pos, requirements: Requirement*)
                                    (implicit dropAt: DropAt.Aux[A, Pos, R], op: HKernelAux[R], toInt: ToInt[Pos]): HListQuery[R] =
        DropElem(q, pos, requirements.toSet)(dropAt, op, toInt)
    def or[B <: HList, R <: HList](q2: HListQuery[B], requirements: Requirement*)
                                  (implicit disjunct: Disjunct.Aux[A, B, R], op: HKernelAux[R]): HListQuery[R] =
        Disjunction(q, q2, requirements.toSet)(disjunct, op)
  }

  // These Case classes are needed because otherwise toFunEventBoolean[A] will always be invoked which is not the wanted behavior
  // TODO is there a better way?
  case class Query1Helper[A](q: HListQuery[A :: HNil]) {
    def where(cond: A => Boolean, requirements: Requirement*)
             (implicit op: HKernelAux[A :: HNil]): HListQuery[A :: HNil] =
      Filter(q, toFunEventBoolean[A](cond), requirements.toSet)(op)
  }

  case class Query2Helper[A, B](q: HListQuery[A :: B :: HNil]) {
    def where(cond: (A, B) => Boolean, requirements: Requirement*)
             (implicit op: HKernelAux[A :: B :: HNil]): HListQuery[A :: B :: HNil] =
      Filter(q, toFunEventBoolean[A, B](cond), requirements.toSet)(op)
  }

  case class Query3Helper[A, B, C](q: HListQuery[A :: B :: C :: HNil]) {
    def where(cond: (A, B, C) => Boolean, requirements: Requirement*)
             (implicit op: HKernelAux[A :: B :: C :: HNil]): HListQuery[A :: B :: C :: HNil] =
      Filter(q, toFunEventBoolean[A, B, C](cond), requirements.toSet)(op)
  }

  case class Query4Helper[A, B, C, D](q: HListQuery[A :: B :: C :: D :: HNil]) {
    def where(cond: (A, B, C, D) => Boolean, requirements: Requirement*)
             (implicit op: HKernelAux[A :: B :: C :: D :: HNil]): HListQuery[A :: B :: C :: D :: HNil] =
      Filter(q, toFunEventBoolean[A, B, C, D](cond), requirements.toSet)(op)
  }

  case class Query5Helper[A, B, C, D, E](q: HListQuery[A :: B :: C :: D :: E :: HNil]) {
    def where(cond: (A, B, C, D, E) => Boolean, requirements: Requirement*)
             (implicit op: HKernelAux[A :: B :: C :: D :: E :: HNil]): HListQuery[A :: B :: C :: D :: E :: HNil] =
      Filter(q, toFunEventBoolean[A, B, C, D, E](cond), requirements.toSet)(op)
  }

  case class Query6Helper[A, B, C, D, E, F](q: HListQuery[A :: B :: C :: D :: E :: F :: HNil]) {
    def where(cond: (A, B, C, D, E, F) => Boolean, requirements: Requirement*)
             (implicit op: HKernelAux[A :: B :: C :: D :: E :: F :: HNil]): HListQuery[A :: B :: C :: D :: E :: F :: HNil] =
      Filter(q, toFunEventBoolean[A, B, C, D, E, F](cond), requirements.toSet)(op)
  }

  implicit def query1ToQuery1Helper[A](q: HListQuery[A::HNil]): Query1Helper[A] = Query1Helper(q)
  implicit def query2ToQuery2Helper[A, B](q: HListQuery[A::B::HNil]): Query2Helper[A, B] = Query2Helper(q)
  implicit def query3ToQuery3Helper[A, B, C](q: HListQuery[A::B::C::HNil]): Query3Helper[A, B, C] = Query3Helper(q)
  implicit def query4ToQuery4Helper[A, B, C, D](q: HListQuery[A::B::C::D::HNil]): Query4Helper[A, B, C, D] = Query4Helper(q)
  implicit def query5ToQuery5Helper[A, B, C, D, E](q: HListQuery[A::B::C::D::E::HNil]): Query5Helper[A, B, C, D, E] = Query5Helper(q)
  implicit def query6ToQuery6Helper[A, B, C, D, E, F](q: HListQuery[A::B::C::D::E::F::HNil]): Query6Helper[A, B, C, D, E, F] = Query6Helper(q)
}