package adaptivecep.data

import java.time.Duration

import adaptivecep.data.Events._
import util.tuplehlistsupport._
import util.records._
import akka.actor.ActorContext
import shapeless.{HList, Nat, Witness}
import shapeless.ops.hlist.HKernelAux
import shapeless.ops.nat.ToInt
import shapeless.ops.record.{Remover, UnzipFields}

object Queries {

  case class NStream[T](publisherName: String)(implicit lengthImplicit: Length[T]) {
    val length: Int = lengthImplicit()
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

  case class NodeData(name: String, query: IQuery, context: ActorContext)

  sealed trait Requirement
  case class LatencyRequirement   (operator: Operator, duration: Duration,           callback: NodeData => Any) extends Requirement
  case class FrequencyRequirement (operator: Operator, instances: Int, seconds: Int, callback: NodeData => Any) extends Requirement

  sealed trait IQuery { val requirements: Set[Requirement] }

  sealed trait LeafQuery   extends IQuery
  sealed trait UnaryQuery  extends IQuery { val sq: IQuery }
  sealed trait BinaryQuery extends IQuery { val sq1: IQuery; val sq2: IQuery }

  sealed trait StreamQuery extends LeafQuery { val publisherName: String }
  sealed trait SequenceQuery[A, B] extends LeafQuery {
    val s1: NStream[A]
    val s2: NStream[B]
  }
  sealed trait FilterQuery      extends UnaryQuery  { val cond: Event => Boolean }
  sealed trait DropElemQuery    extends UnaryQuery
  sealed trait SelfJoinQuery    extends UnaryQuery  { val w1: Window; val w2: Window }
  sealed trait JoinQuery        extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait JoinOnQuery      extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait ConjunctionQuery extends BinaryQuery
  sealed trait DisjunctionQuery extends BinaryQuery

  abstract class Query[T](implicit lengthImplicit: Length[T]) extends IQuery {
    val length: Int = lengthImplicit()
  }

  case class Stream[T](
      publisherName: String,
      requirements: Set[Requirement])
    (implicit lengthImplicit: Length[T]) extends Query[T] with StreamQuery

  case class Sequence[A, B, R](
      s1: NStream[A],
      s2: NStream[B],
      requirements: Set[Requirement])
    (implicit
      prepend: Prepend.Aux[A, B, R],
      length: Length[R]
  ) extends Query[R] with SequenceQuery[A, B]

  case class Filter[T](
      sq: Query[T],
      cond: Event => Boolean,
      requirements: Set[Requirement])
    (implicit
      length: Length[T]
  ) extends Query[T] with FilterQuery

  case class FilterRecord[Labeled <: HList, K <: HList, V <: HList](
      sq: Query[Labeled],
      cond: Event => Boolean,
      requirements: Set[Requirement])
    (implicit
      unzip: UnzipFields.Aux[Labeled, K, V],
      op: HKernelAux[Labeled]
  ) extends Query[Labeled] with FilterQuery

  case class DropElem[T, R, Pos <: Nat](
      sq: Query[T],
      position: Nat,
      requirements: Set[Requirement])
    (implicit
      dropAt: DropAt.Aux[T, Pos, R],
      length: Length[R],
      toInt: ToInt[Pos]
  ) extends Query[R] with DropElemQuery { val pos = toInt() - 1 }

  // Sadly I could not get a type class working that does the dropping at the value and the type level at the same time.
  // Thus, we need the Remover (Type-Level) in addition to DropKey(Value-Level).
  case class DropElemRecord[T <: HList, R <: HList, K, V](
      sq: Query[T],
      k: Witness.Aux[K],
      requirements: Set[Requirement])
    (implicit
      drop: DropKey[T, K],
      remove: Remover.Aux[T, K, (V, R)],
      op: HKernelAux[R]
  ) extends Query[R] with DropElemQuery { val dropKey: DropKey[T, K] = drop }

  case class SelfJoin[T, R](
      sq: Query[T],
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      prepend: Prepend.Aux[T, T, R],
      length: Length[R]
  ) extends Query[R] with SelfJoinQuery

  case class Join[A, B, R](
      sq1: Query[A],
      sq2: Query[B],
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      prepend: Prepend.Aux[A, B, R],
      length: Length[R]
  ) extends Query[R] with JoinQuery

  case class JoinOn[A, B, R, Pos1 <: Nat, Pos2 <: Nat](
      sq1: Query[A],
      sq2: Query[B],
      pos1: Nat,
      pos2: Nat,
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      joinOn: JoinOnNat.Aux[A, B, Pos1, Pos2, R],
      toInt1: ToInt[Pos1],
      toInt2: ToInt[Pos2],
      length: Length[R]
  ) extends Query[R] with JoinOnQuery {
    val positionOn1: Int = toInt1() - 1
    val positionOn2: Int = toInt2() - 1
  }

  case class JoinOnRecord[A <: HList, B <: HList, R <: HList, Key1, Key2](
      sq1: Query[A],
      sq2: Query[B],
      key1: Witness.Aux[Key1],
      key2: Witness.Aux[Key2],
      w1: Window,
      w2: Window,
      requirements: Set[Requirement])
    (implicit
      joinOn: JoinOnKey.Aux[A, B, Key1, Key2, R],
      dropKey: DropKey[B, Key2],
      select1: SelectFromTraversable[A, Key1],
      select2: SelectFromTraversable[B, Key2],
      op: HKernelAux[R]
  ) extends Query[R] with JoinOnQuery {
      val drop: DropKey[B, Key2] = dropKey
      val selectFrom1: SelectFromTraversable[A, Key1] = select1
      val selectFrom2: SelectFromTraversable[B, Key2] = select2
  }

  case class Conjunction[A, B, R](
      sq1: Query[A],
      sq2: Query[B],
      requirements: Set[Requirement])
    (implicit
      prepend: Prepend.Aux[A, B, R],
      length: Length[R]
  ) extends Query[R] with ConjunctionQuery

  case class Disjunction[A, B, R](
      sq1: Query[A],
      sq2: Query[B],
      requirements: Set[Requirement])
    (implicit
      disjunct: Disjunct.Aux[A, B, R],
      length: Length[R]
  ) extends Query[R] with DisjunctionQuery
}
