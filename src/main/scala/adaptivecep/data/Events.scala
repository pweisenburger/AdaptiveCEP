package adaptivecep.data

import akka.actor.ActorRef
import shapeless.HList
import shapeless.ops.hlist.{HKernelAux, ZipWithKeys}
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable._

object Events {

  case object Created

  case object DependenciesRequest
  case class DependenciesResponse(dependencies: Seq[ActorRef])

  case class Event(es: Any*)

  val errorMsg: String = "Panic! Control flow should never reach this point!"

  def toFunEventAny[T <: HList](f: (T) => Any)(implicit
      op: HKernelAux[T],
      ft: FromTraversable[T]): Event => Any = {
    case Event(es@_*) if es.length == op().length => es.toHList[T](ft) match {
      case Some(hlist) => f(hlist)
      case None => sys.error(errorMsg)
    }
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[T <: HList](f: (T) => Boolean)(implicit
      op: HKernelAux[T],
      ft: FromTraversable[T]): Event => Boolean = {
    case Event(es@_*) if es.length == op().length => es.toHList[T](ft) match {
      case Some(hlist) => f(hlist)
      case None => sys.error(errorMsg)
    }
    case _ => sys.error(errorMsg)
  }

  // Need this version for records.
  // Otherwise the value for the implicit parameter ft cannot be found.
  // The reason is that Typable cannot be found for FieldType[Witness.`'key`.T, ValueType]
  def toFunEventBoolean[Labeled <: HList, K <: HList, V <: HList](f: (Labeled) => Boolean)(implicit
      zipWithKeys: ZipWithKeys.Aux[K, V, Labeled],
      op: HKernelAux[Labeled],
      ft: FromTraversable[V]): Event => Boolean = {
    case Event(es@_*) if es.length == op().length =>
      es.toHList[V](ft) match {
        case Some(hlist) => f(zipWithKeys.apply(hlist))
        case None => sys.error(errorMsg)
      }
    case _ => sys.error(errorMsg)
  }
}
