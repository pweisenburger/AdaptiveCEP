package adaptivecep.data

import akka.actor.ActorRef
import shapeless.HList
import shapeless.ops.hlist.HKernelAux
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable._

object Events {

  case object Created

  case object DependenciesRequest
  case class DependenciesResponse(dependencies: Seq[ActorRef])

  case class Event(es: Any*)

  val errorMsg: String = "Panic! Control flow should never reach this point!"

  def toFunEventAny[T <: HList](f: (T) => Any)
                               (implicit op: HKernelAux[T], fl: FromTraversable[T]): Event => Any = {
    case Event(es @ _*) if es.length == op().length => es.toHList[T]  match {
      case Some(hlist) => f(hlist)
      case None => sys.error(errorMsg)
    }
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[T <: HList](f: (T) => Boolean)
                                   (implicit op: HKernelAux[T], fl: FromTraversable[T]): Event => Boolean = {
    case Event(es @ _*) if es.length == op().length => es.toHList[T]  match {
      case Some(hlist) => f(hlist)
      case None => sys.error(errorMsg)
    }
    case _ => sys.error(errorMsg)
  }
}
