package adaptivecep.data

import akka.actor.ActorRef

object Events {

  case object Created

  case object DependenciesRequest
  case class DependenciesResponse(dependencies: Seq[ActorRef])

  case class Event(es: Any*)

  val errorMsg: String = "Panic! Control flow should never reach this point!"

  def toFunEventAny[A](f: (A) => Any): Event => Any = {
    case Event(e1) => f.asInstanceOf[(Any) => Any](e1)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B](f: (A, B) => Any): Event => Any = {
    case Event(e1, e2) => f.asInstanceOf[(Any, Any) => Any](e1, e2)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C](f: (A, B, C) => Any): Event => Any = {
    case Event(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Any](e1, e2, e3)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D](f: (A, B, C, D) => Any): Event => Any = {
    case Event(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Any](e1, e2, e3, e4)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D, E](f: (A, B, C, D, E) => Any): Event => Any = {
    case Event(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Any): Event => Any = {
    case Event(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5, e6)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A](f: (A) => Boolean): Event => Boolean = {
    case Event(e1) => f.asInstanceOf[(Any) => Boolean](e1)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B](f: (A, B) => Boolean): Event => Boolean = {
    case Event(e1, e2) => f.asInstanceOf[(Any, Any) => Boolean](e1, e2)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C](f: (A, B, C) => Boolean): Event => Boolean = {
    case Event(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Boolean](e1, e2, e3)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D](f: (A, B, C, D) => Boolean): Event => Boolean = {
    case Event(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Boolean](e1, e2, e3, e4)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D, E](f: (A, B, C, D, E) => Boolean): Event => Boolean = {
    case Event(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Boolean](e1, e2, e3, e4, e5)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Boolean): Event => Boolean = {
    case Event(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Boolean](e1, e2, e3, e4, e5, e6)
    case _ => sys.error(errorMsg)
  }
}
