package adaptivecep.data

import util.tuplehlistsupport.{FromTraversable, Length}
import akka.actor.ActorRef
import shapeless.HList
import shapeless.ops.hlist.{HKernelAux, ZipWithKeys}
import shapeless.ops.traversable.{FromTraversable => TFromTraversbale}

object Events {

  case object Created

  case object DependenciesRequest
  case class DependenciesResponse(dependencies: Seq[ActorRef])

  case class Event(es: Any*)

  val errorMsg: String = "Panic! Control flow should never reach this point!"

  def toFunEventAny[T](f: (T) => Any)(implicit
      length: Length[T],
      ft: FromTraversable[T]): Event => Any = {
    case Event(es@_*) if es.length == length() => ft(es) match {
      case Some(hlist) => f(hlist)
      case None => sys.error(errorMsg)
    }
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[T](f: (T) => Boolean)(implicit
      length: Length[T],
      ft: FromTraversable[T]): Event => Boolean = {
    case Event(es@_*) if es.length == length() => ft(es) match {
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
      ft: TFromTraversbale[V]): Event => Boolean = {
    case Event(es@_*) if es.length == op().length =>
      ft(es) match {
        case Some(hlist) => f(zipWithKeys(hlist))
        case None => sys.error(errorMsg)
      }
    case _ => sys.error(errorMsg)
  }
}
