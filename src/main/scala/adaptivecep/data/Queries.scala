package adaptivecep.data

import java.time.Duration

import adaptivecep.data.Events._
import akka.actor.ActorContext
import shapeless.{HList, Nat, Witness}
import shapeless.ops.hlist.{At, HKernelAux, Prepend}
import shapeless.ops.nat.{Pred, ToInt}
import shapeless.ops.record.{Remover, Selector, UnzipFields}

object Queries {

  trait NStream {
    def publisherName: String
  }
  case class HListNStream[T <: HList](publisherName: String)(implicit op: HKernelAux[T]) extends NStream {
    val length: Int = op().length
  }

  sealed trait Window
  case class SlidingInstances  (instances: Int) extends Window
  case class TumblingInstances (instances: Int) extends Window
  case class SlidingTime       (seconds: Int)   extends Window
  case class TumblingTime      (seconds: Int)   extends Window

  sealed trait Operator
  case object Equal        extends Operator
  case object NotEqual     extends Operator
  case object Greater      extends Operator
  case object GreaterEqual extends Operator
  case object Smaller      extends Operator
  case object SmallerEqual extends Operator

  case class NodeData(name: String, query: Query, context: ActorContext)

  sealed trait Requirement
  case class LatencyRequirement   (operator: Operator, duration: Duration,           callback: NodeData => Any) extends Requirement
  case class FrequencyRequirement (operator: Operator, instances: Int, seconds: Int, callback: NodeData => Any) extends Requirement

  sealed trait Query { val requirements: Set[Requirement] }

  sealed trait LeafQuery   extends Query
  sealed trait UnaryQuery  extends Query { val sq: Query }
  sealed trait BinaryQuery extends Query { val sq1: Query; val sq2: Query }

  sealed trait StreamQuery      extends LeafQuery   { val publisherName: String }
  sealed trait SequenceQuery[A <: HList, B <: HList] extends LeafQuery   {
    val s1: HListNStream[A]
    val s2: HListNStream[B]
  }
  sealed trait FilterQuery      extends UnaryQuery  { val cond: Event => Boolean }
  sealed trait DropElemQuery    extends UnaryQuery
  sealed trait SelfJoinQuery    extends UnaryQuery  { val w1: Window; val w2: Window }
  sealed trait JoinQuery        extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait JoinOnQuery      extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait ConjunctionQuery extends BinaryQuery
  sealed trait DisjunctionQuery extends BinaryQuery

  abstract class HListQuery[T <: HList](implicit op: HKernelAux[T]) extends Query {
    val length: Int = op().length
  }

  case class Stream[T <: HList](
      publisherName: String,
      requirements: Set[Requirement])
    (implicit op: HKernelAux[T]) extends HListQuery[T] with StreamQuery

  case class Sequence[A <: HList, B <: HList, R <: HList](
      s1: HListNStream[A],
      s2: HListNStream[B],
      requirements: Set[Requirement])
    (implicit
      p: Prepend.Aux[A, B, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with SequenceQuery[A, B]

  case class FilterRecord[Labeled <: HList, K <: HList, V <: HList](
      sq: HListQuery[Labeled],
      cond: Event => Boolean,
      requirements: Set[Requirement])
    (implicit
      unzip: UnzipFields.Aux[Labeled, K, V],
      op: HKernelAux[Labeled]
  ) extends HListQuery[Labeled] with FilterQuery

  case class Filter[T <: HList]
    (sq: HListQuery[T], cond: Event => Boolean, requirements: Set[Requirement])
    (implicit op: HKernelAux[T]) extends HListQuery[T] with FilterQuery

  case class DropElem[T <: HList, R <: HList, Pos <: Nat](
      sq: HListQuery[T],
      position: Nat,
      requirements: Set[Requirement])
    (implicit
      dropAt: DropAt.Aux[T, Pos, R],
      op: HKernelAux[R],
      toInt: ToInt[Pos]
  ) extends HListQuery[R] with DropElemQuery { val pos = toInt() }

  // Sadly I could not get a type class working that does the dropping at the value and the type level at the same time.
  // Thus, we need the Remover (Type-Level) in addition to DropKey(Value-Level).
  case class DropElemRecord[T <: HList, R <: HList, K, V](
      sq: HListQuery[T],
      k: Witness.Aux[K],
      requirements: Set[Requirement])
    (implicit
      drop: DropKey[T, K],
      remove: Remover.Aux[T, K, (V, R)],
      op: HKernelAux[R]
  ) extends HListQuery[R] with DropElemQuery { val dropKey: DropKey[T, K] = drop }

  case class SelfJoin[T <: HList, R <: HList](
      sq: HListQuery[T],
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
   (implicit
      p: Prepend.Aux[T, T, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with SelfJoinQuery

  case class Join[A <: HList, B <: HList, R <: HList](
      sq1: HListQuery[A],
      sq2: HListQuery[B],
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      p: Prepend.Aux[A, B, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with JoinQuery

  case class JoinOn[A <: HList, B <: HList, Dropped <: HList, R <: HList, Pos1 <: Nat, Pos2 <: Nat, PredPos1 <: Nat, PredPos2 <: Nat, On](
      sq1: HListQuery[A],
      sq2: HListQuery[B],
      pos1: Nat,
      pos2: Nat,
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      predPos1: Pred.Aux[Pos1, PredPos1],
      atSq1: At.Aux[A, PredPos1, On],
      predPos2: Pred.Aux[Pos2, PredPos2],
      atSq2: At.Aux[B, PredPos2, On],
      dropAt: DropAt.Aux[B, Pos2, Dropped],
      prepend: Prepend.Aux[A, Dropped, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with JoinOnQuery

  case class JoinOnRecord[A <: HList, B <: HList, Dropped <: HList, R <: HList, Key1, Key2, V, On](
      sq1: HListQuery[A],
      sq2: HListQuery[B],
      key1: Witness.Aux[Key1],
      key2: Witness.Aux[Key2],
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      atSq1: Selector.Aux[A, Key1, On],
      atSq2: Selector.Aux[B, Key2, On],
      dropKey: DropKey[B, Key2],
      remove: Remover.Aux[B, Key2, (V, Dropped)],
      prepend: Prepend.Aux[A, Dropped, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with JoinOnQuery

  case class Conjunction[A <: HList, B <: HList, R <: HList](
      sq1: HListQuery[A],
      sq2: HListQuery[B],
      requirements: Set[Requirement])
    (implicit
      p: Prepend.Aux[A, B, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with ConjunctionQuery

  case class Disjunction[A <: HList, B <: HList, R <: HList](
      sq1: HListQuery[A],
      sq2: HListQuery[B],
      requirements: Set[Requirement])
    (implicit
      disjunct: Disjunct.Aux[A, B, R],
      op: HKernelAux[R]
  ) extends HListQuery[R] with DisjunctionQuery
}
