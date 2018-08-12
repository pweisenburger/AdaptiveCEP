package adaptivecep.data

import java.time.Duration

import akka.actor.ActorContext
import adaptivecep.data.Events._
import shapeless.ops.hlist.{HKernelAux, Prepend}
import shapeless.{HList,::, HNil}

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
  sealed trait SequenceQuery[A <: HList, B <: HList] extends LeafQuery   { val s1: HListNStream[A]; val s2: HListNStream[B] }
  sealed trait FilterQuery      extends UnaryQuery  { val cond: Event => Boolean }
  sealed trait DropElemQuery    extends UnaryQuery
  sealed trait SelfJoinQuery    extends UnaryQuery  { val w1: Window; val w2: Window }
  sealed trait JoinQuery        extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait ConjunctionQuery extends BinaryQuery
  sealed trait DisjunctionQuery extends BinaryQuery


  abstract class HListQuery[T <: HList](implicit op: HKernelAux[T]) extends Query {
    val length: Int = op().length
  }

  case class Stream[T <: HList]
    (publisherName: String, requirements: Set[Requirement])
    (implicit op: HKernelAux[T]) extends HListQuery[T] with StreamQuery

  case class Sequence[A <: HList, B <: HList, R <: HList]
    (s1: HListNStream[A], s2: HListNStream[B], requirements: Set[Requirement])
    (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]) extends HListQuery[R] with SequenceQuery[A, B]

  case class Filter[T <: HList]
    (sq: HListQuery[T], cond: Event => Boolean, requirements: Set[Requirement])
    (implicit op: HKernelAux[T]) extends HListQuery[T] with FilterQuery

  case class DropElem[T<: HList, R <: HList]
    (sq: HListQuery[T], position: Int, requirements: Set[Requirement])
    (implicit op: HKernelAux[R]) extends HListQuery[R] with DropElemQuery

  case class SelfJoin[T <: HList, R <: HList]
    (sq: HListQuery[T], w1: Window, w2: Window, requirements: Set[Requirement])
    (implicit p: Prepend.Aux[T, T, R], op: HKernelAux[R]) extends HListQuery[R] with SelfJoinQuery

  case class Join[A <: HList, B <: HList, R <: HList]
    (sq1: HListQuery[A], sq2: HListQuery[B], w1: Window, w2: Window, requirements: Set[Requirement])
    (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]) extends HListQuery[R] with JoinQuery

  case class Conjunction[A <: HList, B <: HList, R <: HList]
    (sq1: HListQuery[A], sq2: HListQuery[B], requirements: Set[Requirement])
    (implicit p: Prepend.Aux[A, B, R], op: HKernelAux[R]) extends HListQuery[R] with ConjunctionQuery

  type X = Unit
  // TODO
  // case class Disjunction[A <: HList, B <: HList](sq1: HListQuery[A], sq2: HListQuery[B], requirements: Set[Requirement]) extends HListQuery[] with DisjunctionQuery
  // case class Disjunction12[A, B, C]                            (sq1: Query1[A],                sq2: Query2[B, C],             requirements: Set[Requirement]) extends Query2[Either[A, B], Either[X, C]]                                                         with DisjunctionQuery
  // case class Disjunction13[A, B, C, D]                         (sq1: Query1[A],                sq2: Query3[B, C, D],          requirements: Set[Requirement]) extends Query3[Either[A, B], Either[X, C], Either[X, D]]                                           with DisjunctionQuery
  // case class Disjunction14[A, B, C, D, E]                      (sq1: Query1[A],                sq2: Query4[B, C, D, E],       requirements: Set[Requirement]) extends Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]]                             with DisjunctionQuery
  // case class Disjunction15[A, B, C, D, E, F]                   (sq1: Query1[A],                sq2: Query5[B, C, D, E, F],    requirements: Set[Requirement]) extends Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]]               with DisjunctionQuery
  // case class Disjunction16[A, B, C, D, E, F, G]                (sq1: Query1[A],                sq2: Query6[B, C, D, E, F, G], requirements: Set[Requirement]) extends Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] with DisjunctionQuery
  // case class Disjunction21[A, B, C]                            (sq1: Query2[A, B],             sq2: Query1[C],                requirements: Set[Requirement]) extends Query2[Either[A, C], Either[B, X]]                                                         with DisjunctionQuery
  // case class Disjunction22[A, B, C, D]                         (sq1: Query2[A, B],             sq2: Query2[C, D],             requirements: Set[Requirement]) extends Query2[Either[A, C], Either[B, D]]                                                         with DisjunctionQuery
  // case class Disjunction23[A, B, C, D, E]                      (sq1: Query2[A, B],             sq2: Query3[C, D, E],          requirements: Set[Requirement]) extends Query3[Either[A, C], Either[B, D], Either[X, E]]                                           with DisjunctionQuery
  // case class Disjunction24[A, B, C, D, E, F]                   (sq1: Query2[A, B],             sq2: Query4[C, D, E, F],       requirements: Set[Requirement]) extends Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]]                             with DisjunctionQuery
  // case class Disjunction25[A, B, C, D, E, F, G]                (sq1: Query2[A, B],             sq2: Query5[C, D, E, F, G],    requirements: Set[Requirement]) extends Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]]               with DisjunctionQuery
  // case class Disjunction26[A, B, C, D, E, F, G, H]             (sq1: Query2[A, B],             sq2: Query6[C, D, E, F, G, H], requirements: Set[Requirement]) extends Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] with DisjunctionQuery
  // case class Disjunction31[A, B, C, D]                         (sq1: Query3[A, B, C],          sq2: Query1[D],                requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, X], Either[C, X]]                                           with DisjunctionQuery
  // case class Disjunction32[A, B, C, D, E]                      (sq1: Query3[A, B, C],          sq2: Query2[D, E],             requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, E], Either[C, X]]                                           with DisjunctionQuery
  // case class Disjunction33[A, B, C, D, E, F]                   (sq1: Query3[A, B, C],          sq2: Query3[D, E, F],          requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, E], Either[C, F]]                                           with DisjunctionQuery
  // case class Disjunction34[A, B, C, D, E, F, G]                (sq1: Query3[A, B, C],          sq2: Query4[D, E, F, G],       requirements: Set[Requirement]) extends Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]]                             with DisjunctionQuery
  // case class Disjunction35[A, B, C, D, E, F, G, H]             (sq1: Query3[A, B, C],          sq2: Query5[D, E, F, G, H],    requirements: Set[Requirement]) extends Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]]               with DisjunctionQuery
  // case class Disjunction36[A, B, C, D, E, F, G, H, I]          (sq1: Query3[A, B, C],          sq2: Query6[D, E, F, G, H, I], requirements: Set[Requirement]) extends Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] with DisjunctionQuery
  // case class Disjunction41[A, B, C, D, E]                      (sq1: Query4[A, B, C, D],       sq2: Query1[E],                requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  // case class Disjunction42[A, B, C, D, E, F]                   (sq1: Query4[A, B, C, D],       sq2: Query2[E, F],             requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  // case class Disjunction43[A, B, C, D, E, F, G]                (sq1: Query4[A, B, C, D],       sq2: Query3[E, F, G],          requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]]                             with DisjunctionQuery
  // case class Disjunction44[A, B, C, D, E, F, G, H]             (sq1: Query4[A, B, C, D],       sq2: Query4[E, F, G, H],       requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]]                             with DisjunctionQuery
  // case class Disjunction45[A, B, C, D, E, F, G, H, I]          (sq1: Query4[A, B, C, D],       sq2: Query5[E, F, G, H, I],    requirements: Set[Requirement]) extends Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]]               with DisjunctionQuery
  // case class Disjunction46[A, B, C, D, E, F, G, H, I, J]       (sq1: Query4[A, B, C, D],       sq2: Query6[E, F, G, H, I, J], requirements: Set[Requirement]) extends Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] with DisjunctionQuery
  // case class Disjunction51[A, B, C, D, E, F]                   (sq1: Query5[A, B, C, D, E],    sq2: Query1[F],                requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  // case class Disjunction52[A, B, C, D, E, F, G]                (sq1: Query5[A, B, C, D, E],    sq2: Query2[F, G],             requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  // case class Disjunction53[A, B, C, D, E, F, G, H]             (sq1: Query5[A, B, C, D, E],    sq2: Query3[F, G, H],          requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]]               with DisjunctionQuery
  // case class Disjunction54[A, B, C, D, E, F, G, H, I]          (sq1: Query5[A, B, C, D, E],    sq2: Query4[F, G, H, I],       requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]]               with DisjunctionQuery
  // case class Disjunction55[A, B, C, D, E, F, G, H, I, J]       (sq1: Query5[A, B, C, D, E],    sq2: Query5[F, G, H, I, J],    requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]]               with DisjunctionQuery
  // case class Disjunction56[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query5[A, B, C, D, E],    sq2: Query6[F, G, H, I, J, K], requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] with DisjunctionQuery
  // case class Disjunction61[A, B, C, D, E, F, G]                (sq1: Query6[A, B, C, D, E, F], sq2: Query1[G],                requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  // case class Disjunction62[A, B, C, D, E, F, G, H]             (sq1: Query6[A, B, C, D, E, F], sq2: Query2[G, H],             requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  // case class Disjunction63[A, B, C, D, E, F, G, H, I]          (sq1: Query6[A, B, C, D, E, F], sq2: Query3[G, H, I],          requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  // case class Disjunction64[A, B, C, D, E, F, G, H, I, J]       (sq1: Query6[A, B, C, D, E, F], sq2: Query4[G, H, I, J],       requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] with DisjunctionQuery
  // case class Disjunction65[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query6[A, B, C, D, E, F], sq2: Query5[G, H, I, J, K],    requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] with DisjunctionQuery
  // case class Disjunction66[A, B, C, D, E, F, G, H, I, J, K, L] (sq1: Query6[A, B, C, D, E, F], sq2: Query6[G, H, I, J, K, L], requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] with DisjunctionQuery

}
