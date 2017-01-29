package com.scalarookie.eventscalaTS.dsl

import java.time.Duration

import com.scalarookie.eventscalaTS.data.Queries._

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
    def otherwise(callback: String => Any): FrequencyRequirement =
      FrequencyRequirement(operator, ratio.instances.i, ratio.seconds.i, callback)
  }

  implicit def frToSomeFr(ft: FrequencyRequirement): Option[FrequencyRequirement] = Some(ft)

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
    def otherwise(callback: String => Any): LatencyRequirement = LatencyRequirement(operator, duration, callback)
  }

  implicit def lrToSomeLr(lt: LatencyRequirement): Option[LatencyRequirement] = Some(lt)

  // Windows

  def slidingWindow  (instances: Instances): Window = SlidingInstances  (instances.i)
  def slidingWindow  (seconds: Seconds):     Window = SlidingTime       (seconds.i)
  def tumblingWindow (instances: Instances): Window = TumblingInstances (instances.i)
  def tumblingWindow (seconds: Seconds):     Window = TumblingTime      (seconds.i)

  // General

  def stream[A]                (name: String, qosRequirements: QosRequirement*): Query1[A] =                Stream1 (name, qosRequirements.toSet)
  def stream[A, B]             (name: String, qosRequirements: QosRequirement*): Query2[A, B] =             Stream2 (name, qosRequirements.toSet)
  def stream[A, B, C]          (name: String, qosRequirements: QosRequirement*): Query3[A, B, C] =          Stream3 (name, qosRequirements.toSet)
  def stream[A, B, C, D]       (name: String, qosRequirements: QosRequirement*): Query4[A, B, C, D] =       Stream4 (name, qosRequirements.toSet)
  def stream[A, B, C, D, E]    (name: String, qosRequirements: QosRequirement*): Query5[A, B, C, D, E] =    Stream5 (name, qosRequirements.toSet)
  def stream[A, B, C, D, E, F] (name: String, qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = Stream6 (name, qosRequirements.toSet)

  case class Query1Helper[A](q: Query1[A]) {
    def keepEventsWith      (                           cond: (A) => Boolean,   qosRequirements: QosRequirement*): Query1[A] =                KeepEventsWith1 (q, cond,       qosRequirements.toSet)
    def selfJoin            (                           w1: Window, w2: Window, qosRequirements: QosRequirement*): Query2[A, A] =             SelfJoin11      (q, w1, w2,     qosRequirements.toSet)
    def join[B]             (q2: Query1[B],             w1: Window, w2: Window, qosRequirements: QosRequirement*): Query2[A, B] =             Join11          (q, q2, w1, w2, qosRequirements.toSet)
    def join[B, C]          (q2: Query2[B, C],          w1: Window, w2: Window, qosRequirements: QosRequirement*): Query3[A, B, C] =          Join12          (q, q2, w1, w2, qosRequirements.toSet)
    def join[B, C, D]       (q2: Query3[B, C, D],       w1: Window, w2: Window, qosRequirements: QosRequirement*): Query4[A, B, C, D] =       Join13          (q, q2, w1, w2, qosRequirements.toSet)
    def join[B, C, D, E]    (q2: Query4[B, C, D, E],    w1: Window, w2: Window, qosRequirements: QosRequirement*): Query5[A, B, C, D, E] =    Join14          (q, q2, w1, w2, qosRequirements.toSet)
    def join[B, C, D, E, F] (q2: Query5[B, C, D, E, F], w1: Window, w2: Window, qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = Join15          (q, q2, w1, w2, qosRequirements.toSet)
  }

  case class Query2Helper[A, B](q: Query2[A, B]) {
    def keepEventsWith   (                        cond: (A, B) => Boolean, qosRequirements: QosRequirement*): Query2[A, B] =             KeepEventsWith2   (q, cond,       qosRequirements.toSet)
    def removeElement1   (                                                 qosRequirements: QosRequirement*): Query1[B] =                RemoveElement1Of2 (q,             qosRequirements.toSet)
    def removeElement2   (                                                 qosRequirements: QosRequirement*): Query1[A] =                RemoveElement2Of2 (q,             qosRequirements.toSet)
    def selfJoin         (                        w1: Window, w2: Window,  qosRequirements: QosRequirement*): Query4[A, B, A, B] =       SelfJoin22        (q, w1, w2,     qosRequirements.toSet)
    def join[C]          (q2: Query1[C],          w1: Window, w2: Window,  qosRequirements: QosRequirement*): Query3[A, B, C] =          Join21            (q, q2, w1, w2, qosRequirements.toSet)
    def join[C, D]       (q2: Query2[C, D],       w1: Window, w2: Window,  qosRequirements: QosRequirement*): Query4[A, B, C, D] =       Join22            (q, q2, w1, w2, qosRequirements.toSet)
    def join[C, D, E]    (q2: Query3[C, D, E],    w1: Window, w2: Window,  qosRequirements: QosRequirement*): Query5[A, B, C, D, E] =    Join23            (q, q2, w1, w2, qosRequirements.toSet)
    def join[C, D, E, F] (q2: Query4[C, D, E, F], w1: Window, w2: Window,  qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = Join24            (q, q2, w1, w2, qosRequirements.toSet)
  }

  case class Query3Helper[A, B, C](q: Query3[A, B, C]) {
    def keepEventsWith (                     cond: (A, B, C) => Boolean, qosRequirements: QosRequirement*): Query3[A, B, C] =          KeepEventsWith3   (q, cond,       qosRequirements.toSet)
    def removeElement1 (                                                 qosRequirements: QosRequirement*): Query2[B, C] =             RemoveElement1Of3 (q,             qosRequirements.toSet)
    def removeElement2 (                                                 qosRequirements: QosRequirement*): Query2[A, C] =             RemoveElement2Of3 (q,             qosRequirements.toSet)
    def removeElement3 (                                                 qosRequirements: QosRequirement*): Query2[A, B] =             RemoveElement3Of3 (q,             qosRequirements.toSet)
    def selfJoin       (                     w1: Window, w2: Window,     qosRequirements: QosRequirement*): Query6[A, B, C, A, B, C] = SelfJoin33        (q, w1, w2,     qosRequirements.toSet)
    def join[D]        (q2: Query1[D],       w1: Window, w2: Window,     qosRequirements: QosRequirement*): Query4[A, B, C, D] =       Join31            (q, q2, w1, w2, qosRequirements.toSet)
    def join[D, E]     (q2: Query2[D, E],    w1: Window, w2: Window,     qosRequirements: QosRequirement*): Query5[A, B, C, D, E] =    Join32            (q, q2, w1, w2, qosRequirements.toSet)
    def join[D, E, F]  (q2: Query3[D, E, F], w1: Window, w2: Window,     qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = Join33            (q, q2, w1, w2, qosRequirements.toSet)
  }

  case class Query4Helper[A, B, C, D](q: Query4[A, B, C, D]) {
    def keepEventsWith (                  cond: (A, B, C, D) => Boolean, qosRequirements: QosRequirement*): Query4[A, B, C, D] =       KeepEventsWith4   (q, cond,       qosRequirements.toSet)
    def removeElement1 (                                                 qosRequirements: QosRequirement*): Query3[B, C, D] =          RemoveElement1Of4 (q,             qosRequirements.toSet)
    def removeElement2 (                                                 qosRequirements: QosRequirement*): Query3[A, C, D] =          RemoveElement2Of4 (q,             qosRequirements.toSet)
    def removeElement3 (                                                 qosRequirements: QosRequirement*): Query3[A, B, D] =          RemoveElement3Of4 (q,             qosRequirements.toSet)
    def removeElement4 (                                                 qosRequirements: QosRequirement*): Query3[A, B, C] =          RemoveElement4Of4 (q,             qosRequirements.toSet)
    def join[E]        (q2: Query1[E],    w1: Window, w2: Window,        qosRequirements: QosRequirement*): Query5[A, B, C, D, E] =    Join41            (q, q2, w1, w2, qosRequirements.toSet)
    def join[E, F]     (q2: Query2[E, F], w1: Window, w2: Window,        qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = Join42            (q, q2, w1, w2, qosRequirements.toSet)
  }

  case class Query5Helper[A, B, C, D, E](q: Query5[A, B, C, D, E]) {
    def keepEventsWith (               cond: (A, B, C, D, E) => Boolean, qosRequirements: QosRequirement*): Query5[A, B, C, D, E] =    KeepEventsWith5   (q, cond,       qosRequirements.toSet)
    def removeElement1 (                                                 qosRequirements: QosRequirement*): Query4[B, C, D, E] =       RemoveElement1Of5 (q,             qosRequirements.toSet)
    def removeElement2 (                                                 qosRequirements: QosRequirement*): Query4[A, C, D, E] =       RemoveElement2Of5 (q,             qosRequirements.toSet)
    def removeElement3 (                                                 qosRequirements: QosRequirement*): Query4[A, B, D, E] =       RemoveElement3Of5 (q,             qosRequirements.toSet)
    def removeElement4 (                                                 qosRequirements: QosRequirement*): Query4[A, B, C, E] =       RemoveElement4Of5 (q,             qosRequirements.toSet)
    def removeElement5 (                                                 qosRequirements: QosRequirement*): Query4[A, B, C, D] =       RemoveElement5Of5 (q,             qosRequirements.toSet)
    def join[F]        (q2: Query1[F], w1: Window, w2: Window,           qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = Join51            (q, q2, w1, w2, qosRequirements.toSet)
  }

  case class Query6Helper[A, B, C, D, E, F](q: Query6[A, B, C, D, E, F]) {
    def keepEventsWith (cond: (A, B, C, D, E, F) => Boolean, qosRequirements: QosRequirement*): Query6[A, B, C, D, E, F] = KeepEventsWith6   (q, cond, qosRequirements.toSet)
    def removeElement1 (                                     qosRequirements: QosRequirement*): Query5[B, C, D, E, F]    = RemoveElement1Of6 (q,       qosRequirements.toSet)
    def removeElement2 (                                     qosRequirements: QosRequirement*): Query5[A, C, D, E, F]    = RemoveElement2Of6 (q,       qosRequirements.toSet)
    def removeElement3 (                                     qosRequirements: QosRequirement*): Query5[A, B, D, E, F]    = RemoveElement3Of6 (q,       qosRequirements.toSet)
    def removeElement4 (                                     qosRequirements: QosRequirement*): Query5[A, B, C, E, F]    = RemoveElement4Of6 (q,       qosRequirements.toSet)
    def removeElement5 (                                     qosRequirements: QosRequirement*): Query5[A, B, C, D, F]    = RemoveElement5Of6 (q,       qosRequirements.toSet)
    def removeElement6 (                                     qosRequirements: QosRequirement*): Query5[A, B, C, D, E]    = RemoveElement6Of6 (q,       qosRequirements.toSet)
  }

  implicit def query1ToQuery1Helper[A]                (q: Query1[A]):                Query1Helper[A] =                Query1Helper (q)
  implicit def query2ToQuery2Helper[A, B]             (q: Query2[A, B]):             Query2Helper[A, B] =             Query2Helper (q)
  implicit def query3ToQuery3Helper[A, B, C]          (q: Query3[A, B, C]):          Query3Helper[A, B, C] =          Query3Helper (q)
  implicit def query4ToQuery4Helper[A, B, C, D]       (q: Query4[A, B, C, D]):       Query4Helper[A, B, C, D] =       Query4Helper (q)
  implicit def query5ToQuery5Helper[A, B, C, D, E]    (q: Query5[A, B, C, D, E]):    Query5Helper[A, B, C, D, E] =    Query5Helper (q)
  implicit def query6ToQuery6Helper[A, B, C, D, E, F] (q: Query6[A, B, C ,D, E, F]): Query6Helper[A, B, C, D, E, F] = Query6Helper (q)

}
