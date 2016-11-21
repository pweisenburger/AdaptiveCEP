package com.scalarookie.eventscala.caseclasses

import scala.reflect.ClassTag

sealed trait Query

sealed trait Stream extends Query { val name: String }

case class Stream1[A](name: String)(implicit val ctA: ClassTag[A]) extends Stream
case class Stream2[A, B](name: String)(implicit val ctA: ClassTag[A], val ctB: ClassTag[B]) extends Stream
case class Stream3[A, B, C](name: String)(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C]) extends Stream
case class Stream4[A, B, C, D](name: String)(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D]) extends Stream
case class Stream5[A, B, C, D, E](name: String)(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D], val ctE: ClassTag[E]) extends Stream
case class Stream6[A, B, C, D, E, F](name: String)(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D], val ctE: ClassTag[E], val ctF: ClassTag[F]) extends Stream

case class Join(subquery1: Query, subquery1Window: Window, subquery2: Query, subquery2Window: Window) extends Query

sealed trait Window
case class LengthSliding(length: Int) extends Window
case class LengthTumbling(length: Int) extends Window
case class TimeSliding(secs: Int) extends Window
case class TimeTumbling(secs: Int) extends Window

case class Select(subquery: Query, elementIds: List[Int]) extends Query

object Query {

  def getArrayOfClassesFrom(query: Query): Array[java.lang.Class[_]] = {
    def getArrayOfClassesFromStream(stream: Stream): Array[java.lang.Class[_]] = stream match {
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
      case Join(subquery1, _, subquery2, _) =>
        getArrayOfClassesFrom(subquery1) ++ getArrayOfClassesFrom(subquery2)
      case Select(subquery, elementIds) =>
        val arrayOfClasses = getArrayOfClassesFrom(subquery)
        elementIds.map(id => arrayOfClasses(id - 1)).toArray
    }
  }

}
