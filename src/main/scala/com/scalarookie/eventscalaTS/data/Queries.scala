package com.scalarookie.eventscala.caseclasses

import java.time.Duration

import com.scalarookie.eventscala.caseclasses.Event2._

object Query2 {

  sealed trait Operator
  case object Equal        extends Operator
  case object NotEqual     extends Operator
  case object Greater      extends Operator
  case object GreaterEqual extends Operator
  case object Smaller      extends Operator
  case object SmallerEqual extends Operator

  case class LatencyRequirement   (operator: Operator, duration: Duration,           callback: String => Any)
  case class FrequencyRequirement (operator: Operator, instances: Int, seconds: Int, callback: String => Any)

  sealed trait Query { val fr: Option[FrequencyRequirement]; val lr: Option[LatencyRequirement] }

  sealed trait LeadNode   extends Query
  sealed trait UnaryNode  extends Query { val sq: Query }
  sealed trait BinaryNode extends Query { val sq1: Query; val sq2: Query }

  sealed trait StreamQuery   extends Query
  sealed trait FilterQuery   extends UnaryNode { val cond: Event => Boolean }
  sealed trait SelectQuery   extends UnaryNode
  sealed trait SelfJoinQuery extends UnaryNode
  sealed trait JoinQuery     extends BinaryNode

  sealed trait Query1[A]
  sealed trait Query2[A, B]
  sealed trait Query3[A, B, C]
  sealed trait Query4[A, B, C, D]
  sealed trait Query5[A, B, C, D, E]
  sealed trait Query6[A, B, C, D, E, F]

  case class Stream1[A]                (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query1[A]                with StreamQuery
  case class Stream2[A, B]             (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[A, B]             with StreamQuery
  case class Stream3[A, B, C]          (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, B, C]          with StreamQuery
  case class Stream4[A, B, C, D]       (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, D]       with StreamQuery
  case class Stream5[A, B, C, D, E]    (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E]    with StreamQuery
  case class Stream6[A, B, C, D, E, F] (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with StreamQuery

  case class KeepEventsWith1[A]                (sq: Query1[A],                cond: Event1[A] => Boolean,                fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query1[A]                with FilterQuery
  case class KeepEventsWith2[A, B]             (sq: Query2[A, B],             cond: Event2[A, B] => Boolean,             fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[A, B]             with FilterQuery
  case class KeepEventsWith3[A, B, C]          (sq: Query3[A, B, C],          cond: Event3[A, B, C] => Boolean,          fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, B, C]          with FilterQuery
  case class KeepEventsWith4[A, B, C, D]       (sq: Query4[A, B, C, D],       cond: Event4[A, B, C, D] => Boolean,       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, D]       with FilterQuery
  case class KeepEventsWith5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    cond: Event5[A, B, C, D, E] => Boolean,    fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E]    with FilterQuery
  case class KeepEventsWith6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], cond: Event6[A, B, C, D, E, F] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with FilterQuery

  case class RemoveElement1Of2[A, B]             (sq: Query2[A, B],             fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query1[B]             with SelectQuery
  case class RemoveElement2Of2[A, B]             (sq: Query2[A, B],             fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query1[A]             with SelectQuery
  case class RemoveElement1Of3[A, B, C]          (sq: Query3[A, B, C],          fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[B, C]          with SelectQuery
  case class RemoveElement2Of3[A, B, C]          (sq: Query3[A, B, C],          fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[A, C]          with SelectQuery
  case class RemoveElement3Of3[A, B, C]          (sq: Query3[A, B, C],          fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[A, B]          with SelectQuery
  case class RemoveElement1Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[B, C, D]       with SelectQuery
  case class RemoveElement2Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, C, D]       with SelectQuery
  case class RemoveElement3Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, B, D]       with SelectQuery
  case class RemoveElement4Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, B, C]       with SelectQuery
  case class RemoveElement1Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[B, C, D, E]    with SelectQuery
  case class RemoveElement2Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, C, D, E]    with SelectQuery
  case class RemoveElement3Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, D, E]    with SelectQuery
  case class RemoveElement4Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, E]    with SelectQuery
  case class RemoveElement5Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, D]    with SelectQuery
  case class RemoveElement1Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[B, C, D, E, F] with SelectQuery
  case class RemoveElement2Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, C, D, E, F] with SelectQuery
  case class RemoveElement3Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, D, E, F] with SelectQuery
  case class RemoveElement4Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, E, F] with SelectQuery
  case class RemoveElement5Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, F] with SelectQuery
  case class RemoveElement6Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E] with SelectQuery

  sealed trait Window
  case class SlidingInstances  (instances: Int) extends Window
  case class TumblingInstances (instances: Int) extends Window
  case class SlidingSeconds    (seconds: Int)   extends Window
  case class TumblingSeconds   (seconds: Int)   extends Window

  case class SelfJoin11[A]       (sq: Query1[A],       w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[A, A]             with SelfJoinQuery
  case class SelfJoin22[A, B]    (sq: Query2[A, B],    w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, A, B]       with SelfJoinQuery
  case class SelfJoin33[A, B, C] (sq: Query3[A, B, C], w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, A, B, C] with SelfJoinQuery

  case class Join11[A, B]             (sq1: Query1[A],             sq2: Query1[B],             w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query2[A, B]             with JoinQuery
  case class Join12[A, B, C]          (sq1: Query1[A],             sq2: Query2[B, C],          w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, B, C]          with JoinQuery
  case class Join21[A, B, C]          (sq1: Query2[A, B],          sq2: Query1[C],             w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query3[A, B, C]          with JoinQuery
  case class Join13[A, B, C, D]       (sq1: Query1[A],             sq2: Query3[B, C, D],       w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join22[A, B, C, D]       (sq1: Query2[A, B],          sq2: Query2[C, D],          w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join31[A, B, C, D]       (sq1: Query3[A, B, C],       sq2: Query1[D],             w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join14[A, B, C, D, E]    (sq1: Query1[A],             sq2: Query4[B, C, D, E],    w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join23[A, B, C, D, E]    (sq1: Query2[A, B],          sq2: Query3[C, D, E],       w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join32[A, B, C, D, E]    (sq1: Query3[A, B, C],       sq2: Query2[D, E],          w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join41[A, B, C, D, E]    (sq1: Query4[A, B, C, D],    sq2: Query1[E],             w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join15[A, B, C, D, E, F] (sq1: Query1[A],             sq2: Query5[B, C, D, E, F], w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join24[A, B, C, D, E, F] (sq1: Query2[A, B],          sq2: Query4[C, D, E, F],    w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join33[A, B, C, D, E, F] (sq1: Query3[A, B, C],       sq2: Query3[D, E, F],       w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join42[A, B, C, D, E, F] (sq1: Query4[A, B, C, D],    sq2: Query2[E, F],          w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join51[A, B, C, D, E, F] (sq1: Query5[A, B, C, D, E], sq2: Query1[F],             w1: Window, w2: Window, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]) extends Query6[A, B, C, D, E, F] with JoinQuery

}
