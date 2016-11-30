package com.scalarookie.eventscala

import com.scalarookie.eventscala.caseclasses._

import scala.reflect.ClassTag

package object dsl {

  // Queries in general ------------------------------------------------------------------------------------------------

  case class QueryHelper(query: Query) {
    def join(query2: Query) =
      JoinHelper(query, query2)
    def select(elementIds: List[Int]) =
      Select(query, elementIds)
    def where(tuple: (Operator, Either[Int, Any], Either[Int, Any])) =
      Filter(query, tuple._1, tuple._2, tuple._3)
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

  case class Stream1Helper[A : ClassTag]() { def from(publisherName: String) = Stream1[A](publisherName) }
  case class Stream2Helper[A : ClassTag, B : ClassTag]() { def from(publisherName: String) = Stream2[A, B](publisherName) }
  case class Stream3Helper[A : ClassTag, B : ClassTag, C : ClassTag]() { def from(publisherName: String) = Stream3[A, B, C](publisherName) }
  case class Stream4Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag]() { def from(publisherName: String) = Stream4[A, B, C, D](publisherName) }
  case class Stream5Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag]() { def from(publisherName: String) = Stream5[A, B, C, D, E](publisherName) }
  case class Stream6Helper[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag]() { def from(publisherName: String) = Stream6[A, B, C, D, E, F](publisherName) }

  // Joins -------------------------------------------------------------------------------------------------------------

  case class JoinHelper(query1: Query, query2: Query) {
    def in(window1: Window, window2: Window) = Join(query1, window1, query2, window2)
  }

  case class Seconds(s: Int)
  case class Instances(i: Int)

  case class SecondsHelper(i: Int) { def seconds = Seconds(i) }
  case class InstancesHelper(i: Int) { def instances = Instances(i) }

  implicit def int2SecondsHelper(i: Int): SecondsHelper = SecondsHelper(i)
  implicit def int2InstancesHelper(i: Int): InstancesHelper = InstancesHelper(i)

  def slidingWindow(instances: Instances) = LengthSliding(instances.i)
  def slidingWindow(seconds: Seconds) = TimeSliding(seconds.s)
  def tumblingWindow(instances: Instances) = LengthTumbling(instances.i)
  def tumblingWindow(seconds: Seconds) = TimeTumbling(seconds.s)

  // Select ------------------------------------------------------------------------------------------------------------

  def elements(ids: Int*) = ids.toList

  // Filter ------------------------------------------------------------------------------------------------------------

  def element(id: Int) = Left(id)
  def literal(any: Any) = Right(any)

  case class Operand1Helper(operand1: Either[Int, Any]) {
    def =:= = Operand2Helper(Equal, operand1)
    def !:= = Operand2Helper(NotEqual, operand1)
    def :>: = Operand2Helper(Greater, operand1)
    def >:= = Operand2Helper(GreaterEqual, operand1)
    def :<: = Operand2Helper(Smaller, operand1)
    def <:= = Operand2Helper(SmallerEqual, operand1)
  }

  implicit def operand2Operand1Helper(operand: Either[Int, Any]): Operand1Helper = Operand1Helper(operand)

  case class Operand2Helper(operator: Operator, operand1: Either[Int, Any]) {
    def apply(operand2: Either[Int, Any]) = (operator, operand1, operand2)
  }

}
