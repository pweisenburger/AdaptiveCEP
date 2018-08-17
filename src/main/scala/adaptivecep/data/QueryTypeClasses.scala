package adaptivecep.data

import shapeless.ops.hlist.{Drop, Prepend, Split}
import shapeless.{DepFn1, HList, Nat, Succ, _0}

import scala.annotation.implicitNotFound

/**
  * Type class supporting removal of the element at position ''n'' of this `HList`. Available only if this `HList` has at
  * least ''n'' elements.
  * @author André Pacak
  */
@implicitNotFound("Implicit not found: adaptivecep.data.DropAt[${L}, ${N}]. You requested to drop an element at the position ${N}, but the HList ${L} is too short.")
trait DropAt[L <: HList, N <: Nat] extends DepFn1[L] with Serializable { type Out <: HList }

object DropAt {
  def apply[L <: HList, N <: Nat](implicit dropAt: DropAt[L, N]): Aux[L, N, dropAt.Out] = dropAt

  type Aux[L <: HList, N <: Nat, Out0 <: HList] = DropAt[L, N] { type Out = Out0 }

  implicit def defaultDropAt
    [L <: HList, N <: Nat, Pre <: HList, Suf <: HList, SufDrop <: HList, R <: HList]
  (implicit split: Split.Aux[L, N, Pre, Suf],
   drop: Drop.Aux[Suf, Nat._0, SufDrop],
   prepend: Prepend.Aux[Pre, SufDrop, R]): Aux[L, N, R] = new DropAt[L, N] {
    type Out = prepend.Out
    override def apply(l: L): R = {
      val (pre, suf)  = split(l)
      val dropSuf = drop(suf)
      prepend(pre, dropSuf)
    }
  }
}
