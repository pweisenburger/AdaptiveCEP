package com.scalarookie.eventscala.data

import java.time.Duration

import akka.actor.ActorContext
import com.scalarookie.eventscala.data.Events.Event

object Queries {

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

  sealed trait Window
  case class SlidingInstances  (instances: Int) extends Window
  case class TumblingInstances (instances: Int) extends Window
  case class SlidingTime       (seconds: Int)   extends Window
  case class TumblingTime      (seconds: Int)   extends Window

  sealed trait Query { val requirements: Set[Requirement] }

  sealed trait LeafQuery   extends Query
  sealed trait UnaryQuery  extends Query { val sq: Query }
  sealed trait BinaryQuery extends Query { val sq1: Query; val sq2: Query }

  sealed trait StreamQuery      extends LeafQuery   { val publisherName: String }
  sealed trait FilterQuery      extends UnaryQuery  { val cond: Event => Boolean }
  sealed trait SelectQuery      extends UnaryQuery
  sealed trait SelfJoinQuery    extends UnaryQuery  { val w1: Window; val w2: Window }
  sealed trait JoinQuery        extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait DisjunctionQuery extends BinaryQuery

  sealed trait Query1[A]                extends Query
  sealed trait Query2[A, B]             extends Query
  sealed trait Query3[A, B, C]          extends Query
  sealed trait Query4[A, B, C, D]       extends Query
  sealed trait Query5[A, B, C, D, E]    extends Query
  sealed trait Query6[A, B, C, D, E, F] extends Query

  case class Stream1[A]                (publisherName: String, requirements: Set[Requirement]) extends Query1[A]                with StreamQuery
  case class Stream2[A, B]             (publisherName: String, requirements: Set[Requirement]) extends Query2[A, B]             with StreamQuery
  case class Stream3[A, B, C]          (publisherName: String, requirements: Set[Requirement]) extends Query3[A, B, C]          with StreamQuery
  case class Stream4[A, B, C, D]       (publisherName: String, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with StreamQuery
  case class Stream5[A, B, C, D, E]    (publisherName: String, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with StreamQuery
  case class Stream6[A, B, C, D, E, F] (publisherName: String, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with StreamQuery

  case class KeepEventsWith1[A]                (sq: Query1[A],                cond: Event => Boolean, requirements: Set[Requirement]) extends Query1[A]                with FilterQuery
  case class KeepEventsWith2[A, B]             (sq: Query2[A, B],             cond: Event => Boolean, requirements: Set[Requirement]) extends Query2[A, B]             with FilterQuery
  case class KeepEventsWith3[A, B, C]          (sq: Query3[A, B, C],          cond: Event => Boolean, requirements: Set[Requirement]) extends Query3[A, B, C]          with FilterQuery
  case class KeepEventsWith4[A, B, C, D]       (sq: Query4[A, B, C, D],       cond: Event => Boolean, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with FilterQuery
  case class KeepEventsWith5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    cond: Event => Boolean, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with FilterQuery
  case class KeepEventsWith6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], cond: Event => Boolean, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with FilterQuery

  case class RemoveElement1Of2[A, B]             (sq: Query2[A, B],             requirements: Set[Requirement]) extends Query1[B]             with SelectQuery
  case class RemoveElement2Of2[A, B]             (sq: Query2[A, B],             requirements: Set[Requirement]) extends Query1[A]             with SelectQuery
  case class RemoveElement1Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement]) extends Query2[B, C]          with SelectQuery
  case class RemoveElement2Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement]) extends Query2[A, C]          with SelectQuery
  case class RemoveElement3Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement]) extends Query2[A, B]          with SelectQuery
  case class RemoveElement1Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[B, C, D]       with SelectQuery
  case class RemoveElement2Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[A, C, D]       with SelectQuery
  case class RemoveElement3Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[A, B, D]       with SelectQuery
  case class RemoveElement4Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[A, B, C]       with SelectQuery
  case class RemoveElement1Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[B, C, D, E]    with SelectQuery
  case class RemoveElement2Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, C, D, E]    with SelectQuery
  case class RemoveElement3Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, B, D, E]    with SelectQuery
  case class RemoveElement4Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, B, C, E]    with SelectQuery
  case class RemoveElement5Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, B, C, D]    with SelectQuery
  case class RemoveElement1Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[B, C, D, E, F] with SelectQuery
  case class RemoveElement2Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, C, D, E, F] with SelectQuery
  case class RemoveElement3Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, D, E, F] with SelectQuery
  case class RemoveElement4Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, C, E, F] with SelectQuery
  case class RemoveElement5Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, C, D, F] with SelectQuery
  case class RemoveElement6Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, C, D, E] with SelectQuery

  case class SelfJoin11[A]       (sq: Query1[A],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query2[A, A]             with SelfJoinQuery
  case class SelfJoin22[A, B]    (sq: Query2[A, B],    w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, A, B]       with SelfJoinQuery
  case class SelfJoin33[A, B, C] (sq: Query3[A, B, C], w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, A, B, C] with SelfJoinQuery

  case class Join11[A, B]             (sq1: Query1[A],             sq2: Query1[B],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query2[A, B]             with JoinQuery
  case class Join12[A, B, C]          (sq1: Query1[A],             sq2: Query2[B, C],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query3[A, B, C]          with JoinQuery
  case class Join21[A, B, C]          (sq1: Query2[A, B],          sq2: Query1[C],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query3[A, B, C]          with JoinQuery
  case class Join13[A, B, C, D]       (sq1: Query1[A],             sq2: Query3[B, C, D],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join22[A, B, C, D]       (sq1: Query2[A, B],          sq2: Query2[C, D],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join31[A, B, C, D]       (sq1: Query3[A, B, C],       sq2: Query1[D],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join14[A, B, C, D, E]    (sq1: Query1[A],             sq2: Query4[B, C, D, E],    w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join23[A, B, C, D, E]    (sq1: Query2[A, B],          sq2: Query3[C, D, E],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join32[A, B, C, D, E]    (sq1: Query3[A, B, C],       sq2: Query2[D, E],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join41[A, B, C, D, E]    (sq1: Query4[A, B, C, D],    sq2: Query1[E],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join15[A, B, C, D, E, F] (sq1: Query1[A],             sq2: Query5[B, C, D, E, F], w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join24[A, B, C, D, E, F] (sq1: Query2[A, B],          sq2: Query4[C, D, E, F],    w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join33[A, B, C, D, E, F] (sq1: Query3[A, B, C],       sq2: Query3[D, E, F],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join42[A, B, C, D, E, F] (sq1: Query4[A, B, C, D],    sq2: Query2[E, F],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join51[A, B, C, D, E, F] (sq1: Query5[A, B, C, D, E], sq2: Query1[F],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery

  type X = Unit
  case class Disjunction11[A, B]                               (sq1: Query1[A],                sq2: Query1[B],                requirements: Set[Requirement]) extends Query1[Either[A, B]]                                                                       with DisjunctionQuery
  case class Disjunction12[A, B, C]                            (sq1: Query1[A],                sq2: Query2[B, C],             requirements: Set[Requirement]) extends Query2[Either[A, B], Either[X, C]]                                                         with DisjunctionQuery
  case class Disjunction13[A, B, C, D]                         (sq1: Query1[A],                sq2: Query3[B, C, D],          requirements: Set[Requirement]) extends Query3[Either[A, B], Either[X, C], Either[X, D]]                                           with DisjunctionQuery
  case class Disjunction14[A, B, C, D, E]                      (sq1: Query1[A],                sq2: Query4[B, C, D, E],       requirements: Set[Requirement]) extends Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]]                             with DisjunctionQuery
  case class Disjunction15[A, B, C, D, E, F]                   (sq1: Query1[A],                sq2: Query5[B, C, D, E, F],    requirements: Set[Requirement]) extends Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]]               with DisjunctionQuery
  case class Disjunction16[A, B, C, D, E, F, G]                (sq1: Query1[A],                sq2: Query6[B, C, D, E, F, G], requirements: Set[Requirement]) extends Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] with DisjunctionQuery
  case class Disjunction21[A, B, C]                            (sq1: Query2[A, B],             sq2: Query1[C],                requirements: Set[Requirement]) extends Query2[Either[A, C], Either[B, X]]                                                         with DisjunctionQuery
  case class Disjunction22[A, B, C, D]                         (sq1: Query2[A, B],             sq2: Query2[C, D],             requirements: Set[Requirement]) extends Query2[Either[A, C], Either[B, D]]                                                         with DisjunctionQuery
  case class Disjunction23[A, B, C, D, E]                      (sq1: Query2[A, B],             sq2: Query3[C, D, E],          requirements: Set[Requirement]) extends Query3[Either[A, C], Either[B, D], Either[X, E]]                                           with DisjunctionQuery
  case class Disjunction24[A, B, C, D, E, F]                   (sq1: Query2[A, B],             sq2: Query4[C, D, E, F],       requirements: Set[Requirement]) extends Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]]                             with DisjunctionQuery
  case class Disjunction25[A, B, C, D, E, F, G]                (sq1: Query2[A, B],             sq2: Query5[C, D, E, F, G],    requirements: Set[Requirement]) extends Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]]               with DisjunctionQuery
  case class Disjunction26[A, B, C, D, E, F, G, H]             (sq1: Query2[A, B],             sq2: Query6[C, D, E, F, G, H], requirements: Set[Requirement]) extends Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] with DisjunctionQuery
  case class Disjunction31[A, B, C, D]                         (sq1: Query3[A, B, C],          sq2: Query1[D],                requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, X], Either[C, X]]                                           with DisjunctionQuery
  case class Disjunction32[A, B, C, D, E]                      (sq1: Query3[A, B, C],          sq2: Query2[D, E],             requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, E], Either[C, X]]                                           with DisjunctionQuery
  case class Disjunction33[A, B, C, D, E, F]                   (sq1: Query3[A, B, C],          sq2: Query3[D, E, F],          requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, E], Either[C, F]]                                           with DisjunctionQuery
  case class Disjunction34[A, B, C, D, E, F, G]                (sq1: Query3[A, B, C],          sq2: Query4[D, E, F, G],       requirements: Set[Requirement]) extends Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]]                             with DisjunctionQuery
  case class Disjunction35[A, B, C, D, E, F, G, H]             (sq1: Query3[A, B, C],          sq2: Query5[D, E, F, G, H],    requirements: Set[Requirement]) extends Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]]               with DisjunctionQuery
  case class Disjunction36[A, B, C, D, E, F, G, H, I]          (sq1: Query3[A, B, C],          sq2: Query6[D, E, F, G, H, I], requirements: Set[Requirement]) extends Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] with DisjunctionQuery
  case class Disjunction41[A, B, C, D, E]                      (sq1: Query4[A, B, C, D],       sq2: Query1[E],                requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction42[A, B, C, D, E, F]                   (sq1: Query4[A, B, C, D],       sq2: Query2[E, F],             requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction43[A, B, C, D, E, F, G]                (sq1: Query4[A, B, C, D],       sq2: Query3[E, F, G],          requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction44[A, B, C, D, E, F, G, H]             (sq1: Query4[A, B, C, D],       sq2: Query4[E, F, G, H],       requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]]                             with DisjunctionQuery
  case class Disjunction45[A, B, C, D, E, F, G, H, I]          (sq1: Query4[A, B, C, D],       sq2: Query5[E, F, G, H, I],    requirements: Set[Requirement]) extends Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]]               with DisjunctionQuery
  case class Disjunction46[A, B, C, D, E, F, G, H, I, J]       (sq1: Query4[A, B, C, D],       sq2: Query6[E, F, G, H, I, J], requirements: Set[Requirement]) extends Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] with DisjunctionQuery
  case class Disjunction51[A, B, C, D, E, F]                   (sq1: Query5[A, B, C, D, E],    sq2: Query1[F],                requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction52[A, B, C, D, E, F, G]                (sq1: Query5[A, B, C, D, E],    sq2: Query2[F, G],             requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction53[A, B, C, D, E, F, G, H]             (sq1: Query5[A, B, C, D, E],    sq2: Query3[F, G, H],          requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction54[A, B, C, D, E, F, G, H, I]          (sq1: Query5[A, B, C, D, E],    sq2: Query4[F, G, H, I],       requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]]               with DisjunctionQuery
  case class Disjunction55[A, B, C, D, E, F, G, H, I, J]       (sq1: Query5[A, B, C, D, E],    sq2: Query5[F, G, H, I, J],    requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]]               with DisjunctionQuery
  case class Disjunction56[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query5[A, B, C, D, E],    sq2: Query6[F, G, H, I, J, K], requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] with DisjunctionQuery
  case class Disjunction61[A, B, C, D, E, F, G]                (sq1: Query6[A, B, C, D, E, F], sq2: Query1[G],                requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction62[A, B, C, D, E, F, G, H]             (sq1: Query6[A, B, C, D, E, F], sq2: Query2[G, H],             requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction63[A, B, C, D, E, F, G, H, I]          (sq1: Query6[A, B, C, D, E, F], sq2: Query3[G, H, I],          requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction64[A, B, C, D, E, F, G, H, I, J]       (sq1: Query6[A, B, C, D, E, F], sq2: Query4[G, H, I, J],       requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction65[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query6[A, B, C, D, E, F], sq2: Query5[G, H, I, J, K],    requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] with DisjunctionQuery
  case class Disjunction66[A, B, C, D, E, F, G, H, I, J, K, L] (sq1: Query6[A, B, C, D, E, F], sq2: Query6[G, H, I, J, K, L], requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] with DisjunctionQuery
}
