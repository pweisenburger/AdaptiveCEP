package adaptivecep.dsl

import java.time.Duration

import adaptivecep.data.{Disjunct, DropAt, DropKey}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import shapeless.{HList, Nat, Witness}
import shapeless.ops.hlist.{HKernelAux, Prepend, ZipWithKeys}
import shapeless.ops.nat.ToInt
import shapeless.ops.record.{Remover, UnzipFields}
import shapeless.ops.traversable.FromTraversable

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

  def sequence[A <: HList, B <: HList, R <: HList]
    (tuple: (HListNStream[A], HListNStream[B]), requirements: Requirement*)
    (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]): Sequence[A, B, R] =
      Sequence(tuple._1, tuple._2, requirements.toSet)(p, op)

  implicit def queryToQueryHelper[A <: HList](q: HListQuery[A]): QueryHelper[A] = QueryHelper(q)

  // implict functions to enable the usage of a single where function for HLists and Records
  implicit def toUnlabeledWhere[A <: HList](implicit
      op: HKernelAux[A],
      fl: FromTraversable[A]
  ): (HListQuery[A], A => Boolean, Seq[Requirement]) => HListQuery[A] = {
    case (q, cond, reqs) => Filter[A](q, toFunEventBoolean[A](cond)(op, fl), reqs.toSet)(op)
  }

  // See toFunEventBoolean[Labeled, K, V] why an extra version is needed
  implicit def toLabeledWhere[A <: HList, K <: HList, V <: HList](implicit
      unzip: UnzipFields.Aux[A, K, V],
      zipWithKeys: ZipWithKeys.Aux[K, V, A],
      op: HKernelAux[A],
      fl: FromTraversable[V]
  ): (HListQuery[A], A => Boolean, Seq[Requirement]) => HListQuery[A] = {
    case (q, cond, reqs) =>
      FilterRecord[A, K, V](q, toFunEventBoolean[A, K, V](cond)(zipWithKeys, op, fl), reqs.toSet)(unzip, op)
  }

  case class QueryHelper[A <: HList](q: HListQuery[A]) {
    // Sadly we cannot use FnToProduct in order to make the usage better.
    // If we would use FnToProduct, it would be necessary to attach the
    // complete type information for the arguments so that the compiler can find the implicit parameter.
    def where(cond: A => Boolean, requirements: Requirement*)
             (implicit trans: (HListQuery[A], A => Boolean, Seq[Requirement]) => HListQuery[A]): HListQuery[A] = {
      trans(q, cond, requirements)
    }

    def drop[R <: HList, Pos <: Nat](pos: Pos, requirements: Requirement*)(implicit
        dropAt: DropAt.Aux[A, Pos, R],
        op: HKernelAux[R],
        toInt: ToInt[Pos]): HListQuery[R] =
      DropElem(q, pos, requirements.toSet)(dropAt, op, toInt)

    def dropLabeled[R <: HList, K, V](w: Witness.Aux[K], requirements: Requirement*)(implicit
        dropKey: DropKey[A, K],
        remover: Remover.Aux[A, K, (V, R)],
        op: HKernelAux[R]
    ): HListQuery[R] =
      DropElemRecord(w, q, requirements.toSet)(dropKey, remover, op)

    def selfJoin[R <: HList](w1: Window, w2: Window, requirements: Requirement*)(implicit
        p: Prepend.Aux[A, A, R],
        op: HKernelAux[R]): HListQuery[R] =
      SelfJoin[A, R](q, w1, w2, requirements.toSet)(p, op)

    def join[B <: HList, R <: HList](q2: HListQuery[B], w1: Window, w2: Window, requirements: Requirement*)(implicit
        p: Prepend.Aux[A, B, R],
        op: HKernelAux[R]): HListQuery[R] =
      Join[A, B, R](q, q2, w1, w2, requirements.toSet)(p, op)

    def and[B <: HList, R <: HList](q2: HListQuery[B], requirements: Requirement*)(implicit
        p: Prepend.Aux[A, B, R],
        op: HKernelAux[R]): HListQuery[R] =
      Conjunction(q, q2, requirements.toSet)(p, op)

    def or[B <: HList, R <: HList](q2: HListQuery[B], requirements: Requirement*)(implicit
        disjunct: Disjunct.Aux[A, B, R],
        op: HKernelAux[R]): HListQuery[R] =
      Disjunction(q, q2, requirements.toSet)(disjunct, op)
  }
}