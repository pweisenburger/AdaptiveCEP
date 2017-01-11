package com.scalarookie.eventscala.caseclasses

import java.time.Duration
import scala.reflect.ClassTag

case class LatencyRequirement(operator: Operator, duration: Duration, callback: String => Any)
case class FrequencyRequirement(operator: Operator, instances: Int, seconds: Int, callback: String => Any)

sealed trait Query { val frequencyRequirement: Option[FrequencyRequirement]; val latencyRequirement: Option[LatencyRequirement] }
sealed trait LeafQuery extends Query
sealed trait UnaryQuery extends Query { val subquery: Query }
sealed trait BinaryQuery extends Query { val subquery1: Query; val subquery2: Query }

sealed trait Stream extends LeafQuery { val name: String }
case class Stream1[A](name: String, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement])(implicit val ctA: ClassTag[A]) extends Stream
case class Stream2[A, B](name: String, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement])(implicit val ctA: ClassTag[A], val ctB: ClassTag[B]) extends Stream
case class Stream3[A, B, C](name: String, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement])(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C]) extends Stream
case class Stream4[A, B, C, D](name: String, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement])(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D]) extends Stream
case class Stream5[A, B, C, D, E](name: String, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement])(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D], val ctE: ClassTag[E]) extends Stream
case class Stream6[A, B, C, D, E, F](name: String, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement])(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D], val ctE: ClassTag[E], val ctF: ClassTag[F]) extends Stream

case class Select(subquery: Query, elementIds: List[Int], frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) extends UnaryQuery

case class Filter(subquery: Query, operator: Operator , operand1: Either[Int, Any], operand2: Either[Int, Any], frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) extends UnaryQuery

sealed trait Operator
case object Equal extends Operator
case object NotEqual extends Operator
case object Greater extends Operator
case object GreaterEqual extends Operator
case object Smaller extends Operator
case object SmallerEqual extends Operator

case class SelfJoin(subquery: Query, window1: Window, window2: Window, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) extends UnaryQuery

case class Join(subquery1: Query, window1: Window, subquery2: Query, window2: Window, frequencyRequirement: Option[FrequencyRequirement], latencyRequirement: Option[LatencyRequirement]) extends BinaryQuery

sealed trait Window
case class LengthSliding(instances: Int) extends Window
case class LengthTumbling(instances: Int) extends Window
case class TimeSliding(seconds: Int) extends Window
case class TimeTumbling(seconds: Int) extends Window

object Query {

  def getArrayOfClassesFrom(query: Query): Array[Class[_]] = {
    def getArrayOfClassesFromStream(stream: Stream): Array[Class[_]] = stream match {
      case s: Stream1[_] => Array(s.ctA.runtimeClass)
      case s: Stream2[_, _] => Array(s.ctA.runtimeClass, s.ctB.runtimeClass)
      case s: Stream3[_, _, _] => Array(s.ctA.runtimeClass, s.ctB.runtimeClass, s.ctC.runtimeClass)
      case s: Stream4[_, _, _, _] => Array(s.ctA.runtimeClass, s.ctB.runtimeClass, s.ctC.runtimeClass, s.ctD.runtimeClass)
      case s: Stream5[_, _, _, _, _] => Array(s.ctA.runtimeClass, s.ctB.runtimeClass, s.ctC.runtimeClass, s.ctD.runtimeClass, s.ctE.runtimeClass)
      case s: Stream6[_, _, _, _, _, _] => Array(s.ctA.runtimeClass, s.ctB.runtimeClass, s.ctC.runtimeClass, s.ctD.runtimeClass, s.ctE.runtimeClass, s.ctF.runtimeClass)
    }
    query match {
      case s: Stream =>
        getArrayOfClassesFromStream(s)
      case Join(subquery1, _, subquery2, _, _, _) =>
        getArrayOfClassesFrom(subquery1) ++ getArrayOfClassesFrom(subquery2)
      case SelfJoin(subquery, _, _, _, _) =>
        val arrayOfClasses = getArrayOfClassesFrom(subquery)
        arrayOfClasses ++ arrayOfClasses
      case Select(subquery, elementIds, _, _) =>
        val arrayOfClasses = getArrayOfClassesFrom(subquery)
        elementIds.map(id => arrayOfClasses(id - 1)).toArray
      case Filter(subquery, _, _, _, _, _) =>
        getArrayOfClassesFrom(subquery)
    }
  }

  def addFrequencyRequirement[A : ClassTag](stream1: Stream1[A], frequencyRequirement: FrequencyRequirement): Stream1[A] = Stream1[A](stream1.name, Some(frequencyRequirement), stream1.latencyRequirement)
  def addFrequencyRequirement[A : ClassTag, B : ClassTag](stream2: Stream2[A, B], frequencyRequirement: FrequencyRequirement): Stream2[A, B] = Stream2[A, B](stream2.name, Some(frequencyRequirement), stream2.latencyRequirement)
  def addFrequencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag](stream3: Stream3[A, B, C], frequencyRequirement: FrequencyRequirement): Stream3[A, B, C] = Stream3[A, B, C](stream3.name, Some(frequencyRequirement), stream3.latencyRequirement)
  def addFrequencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag](stream4: Stream4[A, B, C, D], frequencyRequirement: FrequencyRequirement): Stream4[A, B, C, D] = Stream4[A, B, C, D](stream4.name, Some(frequencyRequirement), stream4.latencyRequirement)
  def addFrequencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag](stream5: Stream5[A, B, C, D, E], frequencyRequirement: FrequencyRequirement): Stream5[A, B, C, D, E] = Stream5[A, B, C, D, E](stream5.name, Some(frequencyRequirement), stream5.latencyRequirement)
  def addFrequencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag](stream6: Stream6[A, B, C, D, E, F], frequencyRequirement: FrequencyRequirement): Stream6[A, B, C, D, E, F] = Stream6[A, B, C, D, E, F](stream6.name, Some(frequencyRequirement), stream6.latencyRequirement)
  def addFrequencyRequirement(filter: Filter, frequencyRequirement: FrequencyRequirement): Filter = Filter(filter.subquery, filter.operator, filter.operand1, filter.operand2, Some(frequencyRequirement), filter.latencyRequirement)
  def addFrequencyRequirement(select: Select, frequencyRequirement: FrequencyRequirement): Select = Select(select.subquery, select.elementIds, Some(frequencyRequirement), select.latencyRequirement)
  def addFrequencyRequirement(selfJoin: SelfJoin, frequencyRequirement: FrequencyRequirement): SelfJoin = SelfJoin(selfJoin.subquery, selfJoin.window1, selfJoin.window2, Some(frequencyRequirement), selfJoin.latencyRequirement)
  def addFrequencyRequirement(join: Join, frequencyRequirement: FrequencyRequirement): Join = Join(join.subquery1, join.window1, join.subquery2, join.window2, Some(frequencyRequirement), join.latencyRequirement)

  def addLatencyRequirement[A : ClassTag](stream1: Stream1[A], latencyRequirement: LatencyRequirement): Stream1[A] = Stream1[A](stream1.name, stream1.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement[A : ClassTag, B : ClassTag](stream2: Stream2[A, B], latencyRequirement: LatencyRequirement): Stream2[A, B] = Stream2[A, B](stream2.name, stream2.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag](stream3: Stream3[A, B, C], latencyRequirement: LatencyRequirement): Stream3[A, B, C] = Stream3[A, B, C](stream3.name, stream3.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag](stream4: Stream4[A, B, C, D], latencyRequirement: LatencyRequirement): Stream4[A, B, C, D] = Stream4[A, B, C, D](stream4.name, stream4.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag](stream5: Stream5[A, B, C, D, E], latencyRequirement: LatencyRequirement): Stream5[A, B, C, D, E] = Stream5[A, B, C, D, E](stream5.name, stream5.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement[A : ClassTag, B : ClassTag, C : ClassTag, D : ClassTag, E : ClassTag, F : ClassTag](stream6: Stream6[A, B, C, D, E, F], latencyRequirement: LatencyRequirement): Stream6[A, B, C, D, E, F] = Stream6[A, B, C, D, E, F](stream6.name, stream6.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement(filter: Filter, latencyRequirement: LatencyRequirement): Filter = Filter(filter.subquery, filter.operator, filter.operand1, filter.operand2, filter.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement(select: Select, latencyRequirement: LatencyRequirement): Select = Select(select.subquery, select.elementIds, select.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement(selfJoin: SelfJoin, latencyRequirement: LatencyRequirement): SelfJoin = SelfJoin(selfJoin.subquery, selfJoin.window1, selfJoin.window2, selfJoin.frequencyRequirement, Some(latencyRequirement))
  def addLatencyRequirement(join: Join, latencyRequirement: LatencyRequirement): Join = Join(join.subquery1, join.window1, join.subquery2, join.window2, join.frequencyRequirement, Some(latencyRequirement))

}
