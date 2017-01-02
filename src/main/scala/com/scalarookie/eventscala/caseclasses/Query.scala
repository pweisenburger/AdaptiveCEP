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

}
