package adaptivecep.data

import shapeless.ops.hlist.Patcher
import shapeless.ops.nat.Pred
import shapeless.{::, DepFn1, DepFn2, HList, HNil, Nat}

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

  implicit def defaultDropAt[L <: HList, N <: Nat, Pred <: Nat, R <: HList]
    (implicit pred: Pred.Aux[N, Pred], patch: Patcher.Aux[Pred, Nat._1, L, HNil, R] ): Aux[L, N, R] =
      new DropAt[L, N] {
        type Out = patch.Out
        override def apply(l: L): R = { patch(l, HNil) }
      }
}

/**
  * Type class supporting the disjunction of this hlist and another. It results in a HList that creates for each pair of (L,T) an Either[L, R].
  * If one list is shorter than the other the type that is used is Unit.
  * This type class was inspired by ZipWith
  * @author André Pacak
  */
trait Disjunct[L <: HList, R <: HList] extends DepFn2[L, R] with Serializable { type Out <: HList }

object Disjunct {
  def apply[L <: HList, R <: HList] (implicit disjunct: Disjunct[L, R]): Aux[L, R, disjunct.Out] = disjunct

  type Aux[L <: HList, R <: HList, Out0 <: HList] = Disjunct[L, R] { type Out = Out0 }

  implicit def hnilDisjunctHNil: Aux[HNil, HNil, HNil] =
    new Disjunct[HNil, HNil] {
      type Out = HNil
      def apply(l: HNil, r: HNil): HNil = HNil
    }

  implicit def hnilDisjunctList[RH, RT <: HList, ResultRest <: HList]
    (implicit dis: Disjunct.Aux[HNil, RT, ResultRest]): Aux[HNil, RH :: RT, Either[Unit, RH] :: ResultRest] =
      new Disjunct[HNil, RH :: RT] {
        override type Out = Either[Unit, RH] :: ResultRest
        override def apply(t: HNil, u: RH :: RT): Out = Right(u.head) :: dis(HNil, u.tail)
      }

  implicit def hlistDisjunctNil[LH, LT <: HList, ResultRest <: HList]
    (implicit dis: Disjunct.Aux[LT, HNil, ResultRest]): Aux[LH :: LT, HNil, Either[LH, Unit] :: ResultRest] =
      new Disjunct[LH :: LT, HNil] {
        override type Out = Either[LH, Unit] :: ResultRest
        override def apply(t: LH :: LT, u: HNil): Out = Left(t.head) :: dis(t.tail, HNil)
      }

  implicit def hlistDisjunctList[LH, RH, LT <: HList, RT <: HList, Rest <: HList]
    (implicit disjunction: Disjunct.Aux[LT, RT, Rest]): Aux[LH :: LT, RH :: RT, Either[LH, RH] :: Rest] =
      new Disjunct[LH :: LT, RH :: RT] {
        type Out = Either[LH, RH] :: Rest
        // always choose left but this is normally not the wanted behavior.
        // We only use this type class to encode the disjunction of hlist at type level
        def apply(l: LH :: LT, r: RH :: RT): Out = Left(l.head) :: disjunction(l.tail, r.tail)
      }
}
