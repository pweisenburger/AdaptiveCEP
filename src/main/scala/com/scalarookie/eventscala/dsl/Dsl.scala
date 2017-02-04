package com.scalarookie.eventscala.dsl

import java.time.Duration

import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._

object Dsl {

  // Timespan

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

  // Instances

  case class Instances(i: Int)

  case class InstancesHelper(i: Int) {
    def instances: Instances = Instances(i)
  }

  implicit def intToInstancesHelper(i: Int): InstancesHelper = InstancesHelper(i)

  // FrequencyRequirement

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

  // LatencyRequirement

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

  // Windows

  def slidingWindow  (instances: Instances): Window = SlidingInstances  (instances.i)
  def slidingWindow  (seconds: Seconds):     Window = SlidingTime       (seconds.i)
  def tumblingWindow (instances: Instances): Window = TumblingInstances (instances.i)
  def tumblingWindow (seconds: Seconds):     Window = TumblingTime      (seconds.i)

  // Streams

  def stream[A]                (publisherName: String, requirements: Requirement*): Query1[A] =                Stream1 (publisherName, requirements.toSet)
  def stream[A, B]             (publisherName: String, requirements: Requirement*): Query2[A, B] =             Stream2 (publisherName, requirements.toSet)
  def stream[A, B, C]          (publisherName: String, requirements: Requirement*): Query3[A, B, C] =          Stream3 (publisherName, requirements.toSet)
  def stream[A, B, C, D]       (publisherName: String, requirements: Requirement*): Query4[A, B, C, D] =       Stream4 (publisherName, requirements.toSet)
  def stream[A, B, C, D, E]    (publisherName: String, requirements: Requirement*): Query5[A, B, C, D, E] =    Stream5 (publisherName, requirements.toSet)
  def stream[A, B, C, D, E, F] (publisherName: String, requirements: Requirement*): Query6[A, B, C, D, E, F] = Stream6 (publisherName, requirements.toSet)

  // Operators

  case class Query1Helper[A](q: Query1[A]) {
    def keepEventsWith       (                              cond: (A) => Boolean,   requirements: Requirement*): Query1[A] =                                                                                  KeepEventsWith1 (q, toFunEventBoolean(cond), requirements.toSet)
    def selfJoin             (                              w1: Window, w2: Window, requirements: Requirement*): Query2[A, A] =                                                                               SelfJoin11      (q, w1, w2,                  requirements.toSet)
    def join[B]              (q2: Query1[B],                w1: Window, w2: Window, requirements: Requirement*): Query2[A, B] =                                                                               Join11          (q, q2, w1, w2,              requirements.toSet)
    def join[B, C]           (q2: Query2[B, C],             w1: Window, w2: Window, requirements: Requirement*): Query3[A, B, C] =                                                                            Join12          (q, q2, w1, w2,              requirements.toSet)
    def join[B, C, D]        (q2: Query3[B, C, D],          w1: Window, w2: Window, requirements: Requirement*): Query4[A, B, C, D] =                                                                         Join13          (q, q2, w1, w2,              requirements.toSet)
    def join[B, C, D, E]     (q2: Query4[B, C, D, E],       w1: Window, w2: Window, requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join14          (q, q2, w1, w2,              requirements.toSet)
    def join[B, C, D, E, F]  (q2: Query5[B, C, D, E, F],    w1: Window, w2: Window, requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join15          (q, q2, w1, w2,              requirements.toSet)
    def or[B]                (q2: Query1[B],                                        requirements: Requirement*): Query1[Either[A, B]] =                                                                       Disjunction11   (q, q2,                      requirements.toSet)
    def or[B, C]             (q2: Query2[B, C],                                     requirements: Requirement*): Query2[Either[A, B], Either[X, C]] =                                                         Disjunction12   (q, q2,                      requirements.toSet)
    def or[B, C, D]          (q2: Query3[B, C, D],                                  requirements: Requirement*): Query3[Either[A, B], Either[X, C], Either[X, D]] =                                           Disjunction13   (q, q2,                      requirements.toSet)
    def or[B, C, D, E]       (q2: Query4[B, C, D, E],                               requirements: Requirement*): Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]] =                             Disjunction14   (q, q2,                      requirements.toSet)
    def or[B, C, D, E, F]    (q2: Query5[B, C, D, E, F],                            requirements: Requirement*): Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]] =               Disjunction15   (q, q2,                      requirements.toSet)
    def or[B, C, D, E, F, G] (q2: Query6[B, C, D, E, F, G],                         requirements: Requirement*): Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] = Disjunction16   (q, q2,                      requirements.toSet)

  }

  case class Query2Helper[A, B](q: Query2[A, B]) {
    def keepEventsWith       (                              cond: (A, B) => Boolean, requirements: Requirement*): Query2[A, B] =                                                                               KeepEventsWith2   (q, toFunEventBoolean(cond), requirements.toSet)
    def removeElement1       (                                                       requirements: Requirement*): Query1[B] =                                                                                  RemoveElement1Of2 (q,                          requirements.toSet)
    def removeElement2       (                                                       requirements: Requirement*): Query1[A] =                                                                                  RemoveElement2Of2 (q,                          requirements.toSet)
    def selfJoin             (                              w1: Window, w2: Window,  requirements: Requirement*): Query4[A, B, A, B] =                                                                         SelfJoin22        (q, w1, w2,                  requirements.toSet)
    def join[C]              (q2: Query1[C],                w1: Window, w2: Window,  requirements: Requirement*): Query3[A, B, C] =                                                                            Join21            (q, q2, w1, w2,              requirements.toSet)
    def join[C, D]           (q2: Query2[C, D],             w1: Window, w2: Window,  requirements: Requirement*): Query4[A, B, C, D] =                                                                         Join22            (q, q2, w1, w2,              requirements.toSet)
    def join[C, D, E]        (q2: Query3[C, D, E],          w1: Window, w2: Window,  requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join23            (q, q2, w1, w2,              requirements.toSet)
    def join[C, D, E, F]     (q2: Query4[C, D, E, F],       w1: Window, w2: Window,  requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join24            (q, q2, w1, w2,              requirements.toSet)
    def or[C]                (q2: Query1[C],                                         requirements: Requirement*): Query2[Either[A, C], Either[B, X]] =                                                         Disjunction21     (q, q2,                      requirements.toSet)
    def or[C, D]             (q2: Query2[C, D],                                      requirements: Requirement*): Query2[Either[A, C], Either[B, D]] =                                                         Disjunction22     (q, q2,                      requirements.toSet)
    def or[C, D, E]          (q2: Query3[C, D, E],                                   requirements: Requirement*): Query3[Either[A, C], Either[B, D], Either[X, E]] =                                           Disjunction23     (q, q2,                      requirements.toSet)
    def or[C, D, E, F]       (q2: Query4[C, D, E, F],                                requirements: Requirement*): Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]] =                             Disjunction24     (q, q2,                      requirements.toSet)
    def or[C, D, E, F, G]    (q2: Query5[C, D, E, F, G],                             requirements: Requirement*): Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]] =               Disjunction25     (q, q2,                      requirements.toSet)
    def or[C, D, E, F, G, H] (q2: Query6[C, D, E, F, G, H],                          requirements: Requirement*): Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] = Disjunction26     (q, q2,                      requirements.toSet)
  }

  case class Query3Helper[A, B, C](q: Query3[A, B, C]) {
    def keepEventsWith       (                              cond: (A, B, C) => Boolean, requirements: Requirement*): Query3[A, B, C] =                                                                            KeepEventsWith3   (q, toFunEventBoolean(cond), requirements.toSet)
    def removeElement1       (                                                          requirements: Requirement*): Query2[B, C] =                                                                               RemoveElement1Of3 (q,                          requirements.toSet)
    def removeElement2       (                                                          requirements: Requirement*): Query2[A, C] =                                                                               RemoveElement2Of3 (q,                          requirements.toSet)
    def removeElement3       (                                                          requirements: Requirement*): Query2[A, B] =                                                                               RemoveElement3Of3 (q,                          requirements.toSet)
    def selfJoin             (                              w1: Window, w2: Window,     requirements: Requirement*): Query6[A, B, C, A, B, C] =                                                                   SelfJoin33        (q, w1, w2,                  requirements.toSet)
    def join[D]              (q2: Query1[D],                w1: Window, w2: Window,     requirements: Requirement*): Query4[A, B, C, D] =                                                                         Join31            (q, q2, w1, w2,              requirements.toSet)
    def join[D, E]           (q2: Query2[D, E],             w1: Window, w2: Window,     requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join32            (q, q2, w1, w2,              requirements.toSet)
    def join[D, E, F]        (q2: Query3[D, E, F],          w1: Window, w2: Window,     requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join33            (q, q2, w1, w2,              requirements.toSet)
    def or[D]                (q2: Query1[D],                                            requirements: Requirement*): Query3[Either[A, D], Either[B, X], Either[C, X]] =                                           Disjunction31     (q, q2,                      requirements.toSet)
    def or[D, E]             (q2: Query2[D, E],                                         requirements: Requirement*): Query3[Either[A, D], Either[B, E], Either[C, X]] =                                           Disjunction32     (q, q2,                      requirements.toSet)
    def or[D, E, F]          (q2: Query3[D, E, F],                                      requirements: Requirement*): Query3[Either[A, D], Either[B, E], Either[C, F]] =                                           Disjunction33     (q, q2,                      requirements.toSet)
    def or[D, E, F, G]       (q2: Query4[D, E, F, G],                                   requirements: Requirement*): Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]] =                             Disjunction34     (q, q2,                      requirements.toSet)
    def or[D, E, F, G, H]    (q2: Query5[D, E, F, G, H],                                requirements: Requirement*): Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]] =               Disjunction35     (q, q2,                      requirements.toSet)
    def or[D, E, F, G, H, I] (q2: Query6[D, E, F, G, H, I],                             requirements: Requirement*): Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] = Disjunction36     (q, q2,                      requirements.toSet)
  }

  case class Query4Helper[A, B, C, D](q: Query4[A, B, C, D]) {
    def keepEventsWith       (                              cond: (A, B, C, D) => Boolean, requirements: Requirement*): Query4[A, B, C, D] =                                                                         KeepEventsWith4   (q, toFunEventBoolean(cond), requirements.toSet)
    def removeElement1       (                                                             requirements: Requirement*): Query3[B, C, D] =                                                                            RemoveElement1Of4 (q,                          requirements.toSet)
    def removeElement2       (                                                             requirements: Requirement*): Query3[A, C, D] =                                                                            RemoveElement2Of4 (q,                          requirements.toSet)
    def removeElement3       (                                                             requirements: Requirement*): Query3[A, B, D] =                                                                            RemoveElement3Of4 (q,                          requirements.toSet)
    def removeElement4       (                                                             requirements: Requirement*): Query3[A, B, C] =                                                                            RemoveElement4Of4 (q,                          requirements.toSet)
    def join[E]              (q2: Query1[E],                w1: Window, w2: Window,        requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join41            (q, q2, w1, w2,              requirements.toSet)
    def join[E, F]           (q2: Query2[E, F],             w1: Window, w2: Window,        requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join42            (q, q2, w1, w2,              requirements.toSet)
    def or[E]                (q2: Query1[E],                                               requirements: Requirement*): Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]] =                             Disjunction41     (q, q2,                      requirements.toSet)
    def or[E, F]             (q2: Query2[E, F],                                            requirements: Requirement*): Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]] =                             Disjunction42     (q, q2,                      requirements.toSet)
    def or[E, F, G]          (q2: Query3[E, F, G],                                         requirements: Requirement*): Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]] =                             Disjunction43     (q, q2,                      requirements.toSet)
    def or[E, F, G, H]       (q2: Query4[E, F, G, H],                                      requirements: Requirement*): Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]] =                             Disjunction44     (q, q2,                      requirements.toSet)
    def or[E, F, G, H, I]    (q2: Query5[E, F, G, H, I],                                   requirements: Requirement*): Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]] =               Disjunction45     (q, q2,                      requirements.toSet)
    def or[E, F, G, H, I, J] (q2: Query6[E, F, G, H, I, J],                                requirements: Requirement*): Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] = Disjunction46     (q, q2,                      requirements.toSet)
  }

  case class Query5Helper[A, B, C, D, E](q: Query5[A, B, C, D, E]) {
    def keepEventsWith       (                              cond: (A, B, C, D, E) => Boolean, requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      KeepEventsWith5   (q, toFunEventBoolean(cond), requirements.toSet)
    def removeElement1       (                                                                requirements: Requirement*): Query4[B, C, D, E] =                                                                         RemoveElement1Of5 (q,                          requirements.toSet)
    def removeElement2       (                                                                requirements: Requirement*): Query4[A, C, D, E] =                                                                         RemoveElement2Of5 (q,                          requirements.toSet)
    def removeElement3       (                                                                requirements: Requirement*): Query4[A, B, D, E] =                                                                         RemoveElement3Of5 (q,                          requirements.toSet)
    def removeElement4       (                                                                requirements: Requirement*): Query4[A, B, C, E] =                                                                         RemoveElement4Of5 (q,                          requirements.toSet)
    def removeElement5       (                                                                requirements: Requirement*): Query4[A, B, C, D] =                                                                         RemoveElement5Of5 (q,                          requirements.toSet)
    def join[F]              (q2: Query1[F],                w1: Window, w2: Window,           requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join51            (q, q2, w1, w2,              requirements.toSet)
    def or[F]                (q2: Query1[F],                                                  requirements: Requirement*): Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]] =               Disjunction51     (q, q2,                      requirements.toSet)
    def or[F, G]             (q2: Query2[F, G],                                               requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]] =               Disjunction52     (q, q2,                      requirements.toSet)
    def or[F, G, H]          (q2: Query3[F, G, H],                                            requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]] =               Disjunction53     (q, q2,                      requirements.toSet)
    def or[F, G, H, I]       (q2: Query4[F, G, H, I],                                         requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]] =               Disjunction54     (q, q2,                      requirements.toSet)
    def or[F, G, H, I, J]    (q2: Query5[F, G, H, I, J],                                      requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]] =               Disjunction55     (q, q2,                      requirements.toSet)
    def or[F, G, H, I, J, K] (q2: Query6[F, G, H, I, J, K],                                   requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] = Disjunction56     (q, q2,                      requirements.toSet)

  }

  case class Query6Helper[A, B, C, D, E, F](q: Query6[A, B, C, D, E, F]) {
    def keepEventsWith       (                              cond: (A, B, C, D, E, F) => Boolean, requirements: Requirement*): Query6[A, B, C, D, E, F]                                                                   = KeepEventsWith6   (q, toFunEventBoolean(cond), requirements.toSet)
    def removeElement1       (                                                                   requirements: Requirement*): Query5[B, C, D, E, F]                                                                      = RemoveElement1Of6 (q,                          requirements.toSet)
    def removeElement2       (                                                                   requirements: Requirement*): Query5[A, C, D, E, F]                                                                      = RemoveElement2Of6 (q,                          requirements.toSet)
    def removeElement3       (                                                                   requirements: Requirement*): Query5[A, B, D, E, F]                                                                      = RemoveElement3Of6 (q,                          requirements.toSet)
    def removeElement4       (                                                                   requirements: Requirement*): Query5[A, B, C, E, F]                                                                      = RemoveElement4Of6 (q,                          requirements.toSet)
    def removeElement5       (                                                                   requirements: Requirement*): Query5[A, B, C, D, F]                                                                      = RemoveElement5Of6 (q,                          requirements.toSet)
    def removeElement6       (                                                                   requirements: Requirement*): Query5[A, B, C, D, E]                                                                      = RemoveElement6Of6 (q,                          requirements.toSet)
    def or[G]                (q2: Query1[G],                                                     requirements: Requirement*): Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] = Disjunction61     (q, q2,                      requirements.toSet)
    def or[G, H]             (q2: Query2[G, H],                                                  requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] = Disjunction62     (q, q2,                      requirements.toSet)
    def or[G, H, I]          (q2: Query3[G, H, I],                                               requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] = Disjunction63     (q, q2,                      requirements.toSet)
    def or[G, H, I, J]       (q2: Query4[G, H, I, J],                                            requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] = Disjunction64     (q, q2,                      requirements.toSet)
    def or[G, H, I, J, K]    (q2: Query5[G, H, I, J, K],                                         requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] = Disjunction65     (q, q2,                      requirements.toSet)
    def or[G, H, I, J, K, L] (q2: Query6[G, H, I, J, K, L],                                      requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] = Disjunction66     (q, q2,                      requirements.toSet)
  }

  implicit def query1ToQuery1Helper[A]                (q: Query1[A]):                Query1Helper[A] =                Query1Helper (q)
  implicit def query2ToQuery2Helper[A, B]             (q: Query2[A, B]):             Query2Helper[A, B] =             Query2Helper (q)
  implicit def query3ToQuery3Helper[A, B, C]          (q: Query3[A, B, C]):          Query3Helper[A, B, C] =          Query3Helper (q)
  implicit def query4ToQuery4Helper[A, B, C, D]       (q: Query4[A, B, C, D]):       Query4Helper[A, B, C, D] =       Query4Helper (q)
  implicit def query5ToQuery5Helper[A, B, C, D, E]    (q: Query5[A, B, C, D, E]):    Query5Helper[A, B, C, D, E] =    Query5Helper (q)
  implicit def query6ToQuery6Helper[A, B, C, D, E, F] (q: Query6[A, B, C ,D, E, F]): Query6Helper[A, B, C, D, E, F] = Query6Helper (q)

}
