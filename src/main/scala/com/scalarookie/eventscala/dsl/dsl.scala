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
    def join(query2: Query, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) =
      JoinHelper(query, query2, frequencyRequirement, latencyRequirement)
    def select(elementIds: List[Int], frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) =
      Select(query, elementIds, frequencyRequirement, latencyRequirement)
    def where(tuple: (Operator, Either[Int, Any], Either[Int, Any]), frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) =
      Filter(query, tuple._1, tuple._2, tuple._3, frequencyRequirement, latencyRequirement)
  }

  implicit def stream2QueryHelper(stream: Stream): QueryHelper = QueryHelper(stream)
  implicit def join2QueryHelper(join: Join): QueryHelper = QueryHelper(join)
  implicit def select2QueryHelper(select: Select): QueryHelper = QueryHelper(select)
  implicit def filter2QueryHelper(filter: Filter): QueryHelper = QueryHelper(filter)

  // Streams -----------------------------------------------------------------------------------------------------------

  def stream[A : ClassTag] = Stream1Helper[A]()
  def stream[A : ClassTag, B : ClassTag] = Stream2Helper[A, B]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag] = Stream3Helper[A, B, C]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag] = Stream4Helper[A, B, C, D]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag] = Stream5Helper[A, B, C, D, E]()
  def stream[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag] = Stream6Helper[A, B, C, D, E, F]()

  case class Stream1Helper[A : ClassTag]() { def from(publisherName: String, frequencyRequirement: Option[FrequencyRequirement]) = Stream1[A](publisherName, frequencyRequirement) }
  case class Stream2Helper[A : ClassTag, B : ClassTag]() { def from(publisherName: String, frequencyRequirement: Option[FrequencyRequirement]) = Stream2[A, B](publisherName, frequencyRequirement) }
  case class Stream3Helper[A : ClassTag, B : ClassTag, C : ClassTag]() { def from(publisherName: String, frequencyRequirement: Option[FrequencyRequirement]) = Stream3[A, B, C](publisherName, frequencyRequirement) }
  case class Stream4Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag]() { def from(publisherName: String, frequencyRequirement: Option[FrequencyRequirement]) = Stream4[A, B, C, D](publisherName, frequencyRequirement) }
  case class Stream5Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag]() { def from(publisherName: String, frequencyRequirement: Option[FrequencyRequirement]) = Stream5[A, B, C, D, E](publisherName, frequencyRequirement) }
  case class Stream6Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag]() { def from(publisherName: String, frequencyRequirement: Option[FrequencyRequirement]) = Stream6[A, B, C, D, E, F](publisherName, frequencyRequirement) }

  // Joins -------------------------------------------------------------------------------------------------------------

  case class JoinHelper(query1: Query, query2: Query, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) {
    def in(window1: Window, window2: Window) = Join(query1, window1, query2, window2, frequencyRequirement, latencyRequirement)
  }

  def slidingWindow(instances: Instances) = LengthSliding(instances.i)
  def slidingWindow(seconds: Seconds) = TimeSliding(seconds.i)
  def tumblingWindow(instances: Instances) = LengthTumbling(instances.i)
  def tumblingWindow(seconds: Seconds) = TimeTumbling(seconds.i)

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
