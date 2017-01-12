package com.scalarookie.eventscala

import java.time.Duration
import scala.reflect.ClassTag
import com.scalarookie.eventscala.caseclasses._

package object dsl {

  // Units of time / instances------------------------------------------------------------------------------------------

  trait Timespan
  case class Nanoseconds(i: Int) extends Timespan
  case class Milliseconds(i: Int) extends Timespan
  case class Seconds(i: Int) extends Timespan

  case class TimespanHelper(i: Int) {
    def nanoseconds: Nanoseconds = Nanoseconds(i)
    def milliseconds: Milliseconds = Milliseconds(i)
    def seconds: Seconds = Seconds(i)
  }

  implicit def int2TimespanHelper(i: Int): TimespanHelper = TimespanHelper(i)

  case class Instances(i: Int)

  case class InstancesHelper(i: Int) {
    def instances: Instances = Instances(i)
  }

  implicit def int2InstancesHelper(i: Int): InstancesHelper = InstancesHelper(i)

  // Queries in general ------------------------------------------------------------------------------------------------

  case class QueryHelper(query: Query) {
    def join(query2: Query) = { require(query != query2); JoinHelper(query, query2, None, None) }
    def selfJoin(query2: Query) = { require(query == query2); SelfJoinHelper(query, None, None) }
    def select(elementIds: List[Int]) = Select(query, elementIds, None, None)
    def where(tuple: (Operator, Either[Int, Any], Either[Int, Any])) = Filter(query, tuple._1, tuple._2, tuple._3, None, None)
    def frequency(frequencyRequirement: FrequencyRequirement): QueryHelper = query match {
      case s@Stream1(_, _, _) => Query.addFrequencyRequirement(s, frequencyRequirement)
      case s@Stream2(_, _, _) => Query.addFrequencyRequirement(s, frequencyRequirement)
      case s@Stream3(_, _, _) => Query.addFrequencyRequirement(s, frequencyRequirement)
      case s@Stream4(_, _, _) => Query.addFrequencyRequirement(s, frequencyRequirement)
      case s@Stream5(_, _, _) => Query.addFrequencyRequirement(s, frequencyRequirement)
      case s@Stream6(_, _, _) => Query.addFrequencyRequirement(s, frequencyRequirement)
      case f@Filter(_, _, _, _, _, _) => Query.addFrequencyRequirement(f, frequencyRequirement)
      case s@Select(_, _, _, _)       => Query.addFrequencyRequirement(s, frequencyRequirement)
      case s@SelfJoin(_, _, _, _, _)  => Query.addFrequencyRequirement(s, frequencyRequirement)
      case j@Join(_, _, _, _, _, _)   => Query.addFrequencyRequirement(j, frequencyRequirement)
    }
    def latency(latencyRequirement: LatencyRequirement): QueryHelper = query match {
      case s@Stream1(_, _, _) => Query.addLatencyRequirement(s, latencyRequirement)
      case s@Stream2(_, _, _) => Query.addLatencyRequirement(s, latencyRequirement)
      case s@Stream3(_, _, _) => Query.addLatencyRequirement(s, latencyRequirement)
      case s@Stream4(_, _, _) => Query.addLatencyRequirement(s, latencyRequirement)
      case s@Stream5(_, _, _) => Query.addLatencyRequirement(s, latencyRequirement)
      case s@Stream6(_, _, _) => Query.addLatencyRequirement(s, latencyRequirement)
      case f@Filter(_, _, _, _, _, _) => Query.addLatencyRequirement(f, latencyRequirement)
      case s@Select(_, _, _, _)       => Query.addLatencyRequirement(s, latencyRequirement)
      case s@SelfJoin(_, _, _, _, _)  => Query.addLatencyRequirement(s, latencyRequirement)
      case j@Join(_, _, _, _, _, _)   => Query.addLatencyRequirement(j, latencyRequirement)
    }
  }

  implicit def join2QueryHelper(join: Join): QueryHelper = QueryHelper(join)
  implicit def selfJoin2QueryHelper(selfJoin: SelfJoin): QueryHelper = QueryHelper(selfJoin)
  implicit def filter2QueryHelper(filter: Filter): QueryHelper = QueryHelper(filter)
  implicit def select2QueryHelper(select: Select): QueryHelper = QueryHelper(select)
  implicit def stream2QueryHelper(stream: Stream): QueryHelper = QueryHelper(stream)

  implicit def queryHelper2Query(queryHelper: QueryHelper) = queryHelper.query

  // Streams -----------------------------------------------------------------------------------------------------------

  def stream[A : ClassTag] = Stream1Helper[A]()
  def stream[A : ClassTag, B : ClassTag] = Stream2Helper[A, B]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag] = Stream3Helper[A, B, C]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag] = Stream4Helper[A, B, C, D]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag] = Stream5Helper[A, B, C, D, E]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag] = Stream6Helper[A, B, C, D, E, F]()

  case class Stream1Helper[A : ClassTag]() { def from(publisherName: String) = Stream1[A](publisherName, None, None) }
  case class Stream2Helper[A : ClassTag, B : ClassTag]() { def from(publisherName: String) = Stream2[A, B](publisherName, None, None) }
  case class Stream3Helper[A : ClassTag, B : ClassTag, C : ClassTag]() { def from(publisherName: String) = Stream3[A, B, C](publisherName, None, None) }
  case class Stream4Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag]() { def from(publisherName: String) = Stream4[A, B, C, D](publisherName, None, None) }
  case class Stream5Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag]() { def from(publisherName: String) = Stream5[A, B, C, D, E](publisherName, None, None) }
  case class Stream6Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag]() { def from(publisherName: String) = Stream6[A, B, C, D, E, F](publisherName, None, None) }

  // Joins -------------------------------------------------------------------------------------------------------------

  case class JoinHelper(query1: Query, query2: Query, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) {
    def in(window1: Window, window2: Window): Join =
      Join(query1, window1, query2, window2, frequencyRequirement, latencyRequirement)
  }

  case class SelfJoinHelper(query: Query, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) {
    def in(window1: Window, window2: Window): SelfJoin =
      SelfJoin(query, window1, window2, frequencyRequirement, latencyRequirement)
  }

  def slidingWindow(instances: Instances): Window = LengthSliding(instances.i)
  def slidingWindow(seconds: Seconds): Window = TimeSliding(seconds.i)
  def tumblingWindow(instances: Instances): Window = LengthTumbling(instances.i)
  def tumblingWindow(seconds: Seconds): Window = TimeTumbling(seconds.i)

  // Select ------------------------------------------------------------------------------------------------------------

  def elements(ids: Int*): List[Int] = ids.toList

  // Filter ------------------------------------------------------------------------------------------------------------

  def element(id: Int) = Left(id)
  def literal(any: Any) = Right(any)

  case class Operand1Helper(operand1: Either[Int, Any]) {
    def === : Operand2Helper = Operand2Helper(Equal, operand1)
    def =!= : Operand2Helper = Operand2Helper(NotEqual, operand1)
    def >   : Operand2Helper = Operand2Helper(Greater, operand1)
    def >=  : Operand2Helper = Operand2Helper(GreaterEqual, operand1)
    def <   : Operand2Helper = Operand2Helper(Smaller, operand1)
    def <=  : Operand2Helper = Operand2Helper(SmallerEqual, operand1)
  }

  implicit def operand2Operand1Helper(operand: Either[Int, Any]): Operand1Helper = Operand1Helper(operand)

  case class Operand2Helper(operator: Operator, operand1: Either[Int, Any]) {
    def apply(operand2: Either[Int, Any]): (Operator, Either[Int, Any], Either[Int, Any]) =
      (operator, operand1, operand2)
  }

  // Latency -----------------------------------------------------------------------------------------------------------

  def timespan(timespan: Timespan): Duration = timespan match {
    case Nanoseconds(i) => Duration.ofNanos(i)
    case Milliseconds(i) => Duration.ofMillis(i)
    case Seconds(i) => Duration.ofSeconds(i)
  }

  def latency: LatencyHelper.type = LatencyHelper

  case object LatencyHelper {
    def === (duration: Duration): LatencyHelper2 = LatencyHelper2(Equal, duration)
    def =!= (duration: Duration): LatencyHelper2 = LatencyHelper2(NotEqual, duration)
    def >   (duration: Duration): LatencyHelper2 = LatencyHelper2(Greater, duration)
    def >=  (duration: Duration): LatencyHelper2 = LatencyHelper2(GreaterEqual, duration)
    def <   (duration: Duration): LatencyHelper2 = LatencyHelper2(Smaller, duration)
    def <=  (duration: Duration): LatencyHelper2 = LatencyHelper2(SmallerEqual, duration)
  }

  case class LatencyHelper2(operator: Operator, duration: Duration) {
    def otherwise(callback: String => Any): LatencyRequirement = LatencyRequirement(operator, duration, callback)
  }

  // Frequency ---------------------------------------------------------------------------------------------------------

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

}
