package adaptivecep.dsl

import java.time.Duration

import adaptivecep.data._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import shapeless.{Generic, HList, Nat, Witness}
import shapeless.ops.hlist.{HKernelAux, Prepend, Tupler, ZipWithKeys}
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

  def nStream[T](
      publisherName: String)
    (implicit
      implicitLength: LengthImplicit[T]
  ): NStream[T] = NStream[T](publisherName)(implicitLength)

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

  def stream[T](
      publisherName: String,
      requirements: Requirement*)
    (implicit
      length: LengthImplicit[T]
  ): HListQuery[T] = Stream(publisherName, requirements.toSet)(length)


  case class SequenceHelper[A <: HList](s: NStream[A]) {
    def ->[B <: HList](s2: NStream[B]): (NStream[A], NStream[B]) = (s, s2)
  }

  implicit def nStreamToSequenceHelper[T <: HList](s: NStream[T]): SequenceHelper[T] = SequenceHelper(s)

  def sequence[A <: HList, B <: HList, R <: HList](
      tuple: (NStream[A], NStream[B]),
      requirements: Requirement*)
    (implicit
      p: PrependImplicit.Aux[A, B, R],
      length: LengthImplicit[R]
  ): Sequence[A, B, R] = Sequence(tuple._1, tuple._2, requirements.toSet)(p, length)

  implicit def queryToQueryHelper[A](q: HListQuery[A]): QueryHelper[A] = QueryHelper(q)

  // implict functions to enable the usage of a single where function for HLists and Records
  implicit def toUnlabeledWhere[A](implicit
      fl: FromTraversableImplicit[A],
      length: LengthImplicit[A]
  ): (HListQuery[A], A => Boolean, Seq[Requirement]) => HListQuery[A] = {
    case (q, cond, reqs) => Filter[A](q, toFunEventBoolean[A](cond)(length, fl), reqs.toSet)(length)
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

  // implict functions to enable the usage of a single drop function based on nats and witnesses
  implicit def toNatDrop[A <: HList, Pos <: Nat, R <: HList](implicit
       dropAt: DropAtImplicit.Aux[A, Pos, R],
       toInt: ToInt[Pos],
       length: LengthImplicit[R]
  ): (HListQuery[A], Pos, Seq[Requirement]) => HListQuery[R] = {
    case (q, pos, reqs) => DropElem(q, pos, reqs.toSet)(dropAt, length, toInt)
  }

  implicit def toWitnessDrop[A <: HList, K, V, R <: HList](implicit
       dropKey: DropKey[A, K],
       remover: Remover.Aux[A, K, (V, R)],
       op: HKernelAux[R]
  ): (HListQuery[A], Witness.Aux[K], Seq[Requirement]) => HListQuery[R] = {
    case (q, pos, reqs) => DropElemRecord(q, pos, reqs.toSet)(dropKey, remover, op)
  }

  // implict functions to enable the usage of a single joinOn function based on nats and witnesses
  implicit def toJoinOnNat[A <: HList, B <: HList, Pos1 <: Nat, Pos2 <: Nat, R <: HList](implicit
       joinOn: JoinOnNat.Aux[A, B, Pos1, Pos2, R],
       toInt1: ToInt[Pos1],
       toInt2: ToInt[Pos2],
       op: HKernelAux[R]
  ): (HListQuery[A], HListQuery[B], Pos1, Pos2, Window, Window, Seq[Requirement]) => HListQuery[R] = {
    case (q1, q2, pos1, pos2, w1, w2, reqs) => JoinOn(q1, q2, pos1, pos2, w1, w2, reqs.toSet)
  }

  implicit def toJoinOnKey[A <: HList, B <: HList, Key1, Key2, R <: HList](implicit
       joinOn: JoinOnKey.Aux[A, B, Key1, Key2, R],
       dropKey: DropKey[B, Key2],
       select1: SelectFromTraversable[A, Key1],
       select2: SelectFromTraversable[B, Key2],
       op: HKernelAux[R]
   ): (HListQuery[A], HListQuery[B], Witness.Aux[Key1], Witness.Aux[Key2], Window, Window, Seq[Requirement]) => HListQuery[R] = {
    case (q1, q2, pos1, pos2, w1, w2, reqs) => JoinOnRecord(q1, q2, pos1, pos2, w1, w2, reqs.toSet)
  }

  case class QueryHelper[A](q: HListQuery[A]) {
    // Sadly we cannot use FnToProduct in order to make the usage better.
    // If we would use FnToProduct, it would be necessary to attach the
    // complete type information for the arguments so that the compiler can find the implicit parameter.
    def where(
        cond: A => Boolean,
        requirements: Requirement*)
      (implicit
        trans: (HListQuery[A], A => Boolean, Seq[Requirement]) => HListQuery[A]
    ): HListQuery[A] = trans(q, cond, requirements)

    def drop[Pos, R](
        toDrop: Pos,
        requirements: Requirement*)
      (implicit
        trans: (HListQuery[A], Pos, Seq[Requirement]) => HListQuery[R]
    ): HListQuery[R] = trans(q, toDrop, requirements)

    def joinOn[B <: HList, Pos1, Pos2, R <: HList](
        q2: HListQuery[B],
        pos1: Pos1,
        pos2: Pos2,
        w1: Window,
        w2: Window,
        requirements: Requirement*)
      (implicit
        trans: (HListQuery[A], HListQuery[B], Pos1, Pos2, Window, Window, Seq[Requirement]) => HListQuery[R]
      ): HListQuery[R] = trans(q, q2, pos1, pos2, w1, w2, requirements)

    def selfJoin[R](
        w1: Window,
        w2: Window,
        requirements: Requirement*)
      (implicit
        prepend: PrependImplicit.Aux[A, A, R],
        length: LengthImplicit[R]
    ): HListQuery[R] = SelfJoin[A, R](q, w1, w2, requirements.toSet)(prepend, length)

    def join[B, R](
        q2: HListQuery[B],
        w1: Window,
        w2: Window,
        requirements: Requirement*)
      (implicit
        prepend: PrependImplicit.Aux[A, B, R],
        length: LengthImplicit[R]
    ): HListQuery[R] = Join[A, B, R](q, q2, w1, w2, requirements.toSet)(prepend, length)

    def and[B, R](
        q2: HListQuery[B],
        requirements: Requirement*)
      (implicit
        prepend: PrependImplicit.Aux[A, B, R],
        length: LengthImplicit[R]
    ): HListQuery[R] = Conjunction(q, q2, requirements.toSet)(prepend, length)

    def or[B, R](
        q2: HListQuery[B],
        requirements: Requirement*)
      (implicit
        disjunct: DisjunctImplicit.Aux[A, B, R],
        length: LengthImplicit[R]
    ): HListQuery[R] = Disjunction(q, q2, requirements.toSet)(disjunct, length)
  }
}
