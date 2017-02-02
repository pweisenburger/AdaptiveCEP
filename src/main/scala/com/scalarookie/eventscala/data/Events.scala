package com.scalarookie.eventscala.data

object Events {

  case object Created

  sealed trait Event
  case class Event1(e1: Any)                                              extends Event
  case class Event2(e1: Any, e2: Any)                                     extends Event
  case class Event3(e1: Any, e2: Any, e3: Any)                            extends Event
  case class Event4(e1: Any, e2: Any, e3: Any, e4: Any)                   extends Event
  case class Event5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any)          extends Event
  case class Event6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any) extends Event

  val errorMsg: String = "Panic! Control flow should never reach this point!"

  def generalize[A](f: (A) => Any): Event => Any = {
    case Event1(e1) => f.asInstanceOf[(Any) => Any](e1)
    case _ => sys.error(errorMsg)
  }

  def generalize[A, B](f: (A, B) => Any): Event => Any = {
    case Event2(e1, e2) => f.asInstanceOf[(Any, Any) => Any](e1, e2)
    case _ => sys.error(errorMsg)
  }

  def generalize[A, B, C](f: (A, B, C) => Any): Event => Any = {
    case Event3(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Any](e1, e2, e3)
    case _ => sys.error(errorMsg)
  }

  def generalize[A, B, C, D](f: (A, B, C, D) => Any): Event => Any = {
    case Event4(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Any](e1, e2, e3, e4)
    case _ => sys.error(errorMsg)
  }

  def generalize[A, B, C, D, E](f: (A, B, C, D, E) => Any): Event => Any = {
    case Event5(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5)
    case _ => sys.error(errorMsg)
  }

  def generalize[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Any): Event => Any = {
    case Event6(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5, e6)
    case _ => sys.error(errorMsg)
  }

}
