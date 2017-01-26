package com.scalarookie.eventscalaTS.dsl

import java.time.Duration
import com.scalarookie.eventscalaTS.data.Events._
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
  def slidingWindow  (seconds: Seconds):     Window = SlidingSeconds    (seconds.i)
  def tumblingWindow (instances: Instances): Window = TumblingInstances (instances.i)
  def tumblingWindow (seconds: Seconds):     Window = TumblingSeconds   (seconds.i)

  // General

  def stream[A]                (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query1[A] =                Stream1 (name, fr, lr)
  def stream[A, B]             (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[A, B] =             Stream2 (name, fr, lr)
  def stream[A, B, C]          (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, B, C] =          Stream3 (name, fr, lr)
  def stream[A, B, C, D]       (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, D] =       Stream4 (name, fr, lr)
  def stream[A, B, C, D, E]    (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E] =    Stream5 (name, fr, lr)
  def stream[A, B, C, D, E, F] (name: String, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = Stream6 (name, fr, lr)

  case class Query1Helper[A](q: Query1[A]) {
    def keepEventsWith      (                           cond: Event1[A] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query1[A] =                KeepEventsWith1 (q, cond, fr, lr)
    def selfJoin            (                           w1: Window, w2: Window,     fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[A, A] =             SelfJoin11      (q, w1, w2, fr, lr)
    def join[B]             (q2: Query1[B],             w1: Window, w2: Window,     fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[A, B] =             Join11          (q, q2, w1, w2, fr, lr)
    def join[B, C]          (q2: Query2[B, C],          w1: Window, w2: Window,     fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, B, C] =          Join12          (q, q2, w1, w2, fr, lr)
    def join[B, C, D]       (q2: Query3[B, C, D],       w1: Window, w2: Window,     fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, D] =       Join13          (q, q2, w1, w2, fr, lr)
    def join[B, C, D, E]    (q2: Query4[B, C, D, E],    w1: Window, w2: Window,     fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E] =    Join14          (q, q2, w1, w2, fr, lr)
    def join[B, C, D, E, F] (q2: Query5[B, C, D, E, F], w1: Window, w2: Window,     fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = Join15          (q, q2, w1, w2, fr, lr)
  }

  case class Query2Helper[A, B](q: Query2[A, B]) {
    def keepEventsWith   (                        cond: Event2[A, B] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[A, B] =             KeepEventsWith2   (q, cond, fr, lr)
    def removeElement1   (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query1[B] =                RemoveElement1Of2 (q, fr, lr)
    def removeElement2   (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query1[A] =                RemoveElement2Of2 (q, fr, lr)
    def selfJoin         (                        w1: Window, w2: Window,        fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, A, B] =       SelfJoin22        (q, w1, w2, fr, lr)
    def join[C]          (q2: Query1[C],          w1: Window, w2: Window,        fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, B, C] =          Join21            (q, q2, w1, w2, fr, lr)
    def join[C, D]       (q2: Query2[C, D],       w1: Window, w2: Window,        fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, D] =       Join22            (q, q2, w1, w2, fr, lr)
    def join[C, D, E]    (q2: Query3[C, D, E],    w1: Window, w2: Window,        fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E] =    Join23            (q, q2, w1, w2, fr, lr)
    def join[C, D, E, F] (q2: Query4[C, D, E, F], w1: Window, w2: Window,        fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = Join24            (q, q2, w1, w2, fr, lr)
  }

  case class Query3Helper[A, B, C](q: Query3[A, B, C]) {
    def keepEventsWith (                     cond: Event3[A, B, C] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, B, C] =          KeepEventsWith3   (q, cond, fr, lr)
    def removeElement1 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[B, C] =             RemoveElement1Of3 (q, fr, lr)
    def removeElement2 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[A, C] =             RemoveElement2Of3 (q, fr, lr)
    def removeElement3 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query2[A, B] =             RemoveElement3Of3 (q, fr, lr)
    def selfJoin       (                     w1: Window, w2: Window,           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, A, B, C] = SelfJoin33        (q, w1, w2, fr, lr)
    def join[D]        (q2: Query1[D],       w1: Window, w2: Window,           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, D] =       Join31            (q, q2, w1, w2, fr, lr)
    def join[D, E]     (q2: Query2[D, E],    w1: Window, w2: Window,           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E] =    Join32            (q, q2, w1, w2, fr, lr)
    def join[D, E, F]  (q2: Query3[D, E, F], w1: Window, w2: Window,           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = Join33            (q, q2, w1, w2, fr, lr)
  }

  case class Query4Helper[A, B, C, D](q: Query4[A, B, C, D]) {
    def keepEventsWith (                  cond: Event4[A, B, C, D] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, D] =       KeepEventsWith4   (q, cond, fr, lr)
    def removeElement1 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[B, C, D] =          RemoveElement1Of4 (q, fr, lr)
    def removeElement2 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, C, D] =          RemoveElement2Of4 (q, fr, lr)
    def removeElement3 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, B, D] =          RemoveElement3Of4 (q, fr, lr)
    def removeElement4 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query3[A, B, C] =          RemoveElement4Of4 (q, fr, lr)
    def join[E]        (q2: Query1[E],    w1: Window, w2: Window,              fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E] =    Join41            (q, q2, w1, w2, fr, lr)
    def join[E, F]     (q2: Query2[E, F], w1: Window, w2: Window,              fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = Join42            (q, q2, w1, w2, fr, lr)
  }

  case class Query5Helper[A, B, C, D, E](q: Query5[A, B, C, D, E]) {
    def keepEventsWith (               cond: Event5[A, B, C, D, E] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E] =    KeepEventsWith5   (q, cond, fr, lr)
    def removeElement1 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[B, C, D, E] =       RemoveElement1Of5 (q, fr, lr)
    def removeElement2 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, C, D, E] =       RemoveElement2Of5 (q, fr, lr)
    def removeElement3 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, D, E] =       RemoveElement3Of5 (q, fr, lr)
    def removeElement4 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, E] =       RemoveElement4Of5 (q, fr, lr)
    def removeElement5 (                                                       fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query4[A, B, C, D] =       RemoveElement5Of5 (q, fr, lr)
    def join[F]        (q2: Query1[F], w1: Window, w2: Window,                 fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = Join51            (q, q2, w1, w2, fr, lr)
  }

  case class Query6Helper[A, B, C, D, E, F](q: Query6[A, B, C, D, E, F]) {
    //def keepEventsWith (cond: Event6[A, B, C, D, E, F] => Boolean, fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query6[A, B, C, D, E, F] = KeepEventsWith6   (q, cond, fr, lr)
    def removeElement1 (                                           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[B, C, D, E, F]    = RemoveElement1Of6 (q, fr, lr)
    def removeElement2 (                                           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, C, D, E, F]    = RemoveElement2Of6 (q, fr, lr)
    def removeElement3 (                                           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, D, E, F]    = RemoveElement3Of6 (q, fr, lr)
    def removeElement4 (                                           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, E, F]    = RemoveElement4Of6 (q, fr, lr)
    def removeElement5 (                                           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, F]    = RemoveElement5Of6 (q, fr, lr)
    def removeElement6 (                                           fr: Option[FrequencyRequirement], lr: Option[LatencyRequirement]): Query5[A, B, C, D, E]    = RemoveElement6Of6 (q, fr, lr)
  }

  implicit def query1ToQuery1Helper[A]                (q: Query1[A]):                Query1Helper[A] =                Query1Helper(q)
  implicit def query2ToQuery2Helper[A, B]             (q: Query2[A, B]):             Query2Helper[A, B] =             Query2Helper(q)
  implicit def query3ToQuery3Helper[A, B, C]          (q: Query3[A, B, C]):          Query3Helper[A, B, C] =          Query3Helper(q)
  implicit def query4ToQuery4Helper[A, B, C, D]       (q: Query4[A, B, C, D]):       Query4Helper[A, B, C, D] =       Query4Helper(q)
  implicit def query5ToQuery5Helper[A, B, C, D, E]    (q: Query5[A, B, C, D, E]):    Query5Helper[A, B, C, D, E] =    Query5Helper(q)
  implicit def query6ToQuery6Helper[A, B, C, D, E, F] (q: Query6[A, B, C ,D, E, F]): Query6Helper[A, B, C, D, E, F] = Query6Helper(q)

  // Demo

  val streamA: Query2[Int, String] = stream[Int, String]("A", None, None) // TODO source("A")
  val streamB: Query1[Boolean] =     stream[Boolean]    ("B", None, None)

  val query: Query3[Int, String, Int] =
    streamA
    .join(
      streamB,
      tumblingWindow(3.instances),
      tumblingWindow(3.instances),
      None,
      None)
    .removeElement3(
      None,
      None)
    .selfJoin(
      slidingWindow(3.seconds),
      slidingWindow(3.seconds),
      None,
      None)
    .keepEventsWith(
      event => event.e1 < event.e3,
      None,
      None)
    .removeElement4(
      frequency >= ratio(3.instances, 5.seconds) otherwise { println },
      latency   <  timespan(100.milliseconds)    otherwise { sys.error }
    )

}
