package com.scalarookie.eventscala.caseclasses

import java.time.Instant
import scala.reflect.ClassTag

sealed trait Event { val timestamp: Instant }
case class Event1[A](timestamp: Instant, t: (A))(implicit val ctA: ClassTag[A]) extends Event
case class Event2[A, B](timestamp: Instant, t: (A, B))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B]) extends Event
case class Event3[A, B, C](timestamp: Instant, t: (A, B, C))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C]) extends Event
case class Event4[A, B, C, D](timestamp: Instant, t: (A, B, C, D))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D]) extends Event
case class Event5[A, B, C, D, E](timestamp: Instant, t: (A, B, C, D, E))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D], val ctE: ClassTag[E]) extends Event
case class Event6[A, B, C, D, E, F](timestamp: Instant, t: (A, B, C, D, E, F))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D], val ctE: ClassTag[E], val ctF: ClassTag[F]) extends Event

object Event {

    def getArrayOfValuesFrom(event: Event): Array[AnyRef] = event match {
    case e: Event1[_] => Array(e.t.asInstanceOf[AnyRef])
    case e: Event2[_, _] => Array(e.t._1.asInstanceOf[AnyRef], e.t._2.asInstanceOf[AnyRef])
    case e: Event3[_, _, _] => Array(e.t._1.asInstanceOf[AnyRef], e.t._2.asInstanceOf[AnyRef], e.t._3.asInstanceOf[AnyRef])
    case e: Event4[_, _, _, _] => Array(e.t._1.asInstanceOf[AnyRef], e.t._2.asInstanceOf[AnyRef], e.t._3.asInstanceOf[AnyRef], e.t._4.asInstanceOf[AnyRef])
    case e: Event5[_, _, _, _, _] => Array(e.t._1.asInstanceOf[AnyRef], e.t._2.asInstanceOf[AnyRef], e.t._3.asInstanceOf[AnyRef], e.t._4.asInstanceOf[AnyRef], e.t._5.asInstanceOf[AnyRef])
    case e: Event6[_, _, _, _, _, _] => Array(e.t._1.asInstanceOf[AnyRef], e.t._2.asInstanceOf[AnyRef], e.t._3.asInstanceOf[AnyRef], e.t._4.asInstanceOf[AnyRef], e.t._5.asInstanceOf[AnyRef], e.t._6.asInstanceOf[AnyRef])
  }

  def getEventFrom(timestamp: Instant, values: Array[AnyRef], classes: Array[Class[_]]): Event = {
    require(values.length == classes.length)
    values.length match {
      case 1 => Event1(timestamp, Tuple1(classes(0).cast(values(0))))
      case 2 => Event2(timestamp, Tuple2(classes(0).cast(values(0)), classes(1).cast(values(1))))
      case 3 => Event3(timestamp, Tuple3(classes(0).cast(values(0)), classes(1).cast(values(1)), classes(2).cast(values(2))))
      case 4 => Event4(timestamp, Tuple4(classes(0).cast(values(0)), classes(1).cast(values(1)), classes(2).cast(values(2)), classes(3).cast(values(3))))
      case 5 => Event5(timestamp, Tuple5(classes(0).cast(values(0)), classes(1).cast(values(1)), classes(2).cast(values(2)), classes(3).cast(values(3)), classes(4).cast(values(4))))
      case 6 => Event6(timestamp, Tuple6(classes(0).cast(values(0)), classes(1).cast(values(1)), classes(2).cast(values(2)), classes(3).cast(values(3)), classes(4).cast(values(4)), classes(5).cast(values(5))))
    }
  }

}
