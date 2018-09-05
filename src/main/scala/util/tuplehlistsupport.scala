package util

import shapeless.ops.hlist.{HKernelAux, Tupler}
import shapeless.ops.{hlist, traversable, tuple}
import shapeless.{Generic, HList, Nat}

import scala.collection.GenTraversable

// These type classes are need in order to support the usage of tuples and hlists within the dsl
object tuplehlistsupport {

  trait Length[T] {
    def apply(): Int
  }

  trait LowPriorityLength {
    implicit def anyLength[T](): Length[T] = () => 1
  }

  object Length extends LowPriorityLength {

    implicit def hlistLength[T <: HList](implicit op: HKernelAux[T]): Length[T] =
      () => op().length

    implicit def tupleLength[T <: Product, R <: HList](implicit gen: Generic.Aux[T, R], op: HKernelAux[R]): Length[T] =
      () => op().length
  }

  trait Prepend[A, B] {
    type Out
  }

  trait LowPriorityPrepend {

    implicit def hlistTuplePrepend[A <: HList, B <: Product, BH <: HList, R <: HList](implicit
        gen: Generic.Aux[B, BH],
        prepend: hlist.Prepend.Aux[A, BH, R]): Prepend.Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }

    implicit def tupleHListPrepend[A <: Product, B <: HList, BP <: Product, R <: Product](implicit
        gen: Tupler.Aux[B, BP],
        prepend: tuple.Prepend.Aux[A, BP, R]): Prepend.Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }

  }

  object Prepend extends LowPriorityPrepend {
    type Aux[A, B, Out0] = Prepend[A, B] { type Out = Out0 }

    implicit def hlistPrepend[A <: HList, B <: HList, R <: HList](implicit prepend: hlist.Prepend.Aux[A, B, R]): Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }


    implicit def tuplePrepend[A <: Product, B <: Product, R <: Product](implicit prepend: tuple.Prepend.Aux[A, B, R]): Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }
  }

  trait DropAt[A, Pos <: Nat] {
    type Out
  }

  object DropAt {
    type Aux[A, Pos <: Nat, Out0] = DropAt[A, Pos] { type Out = Out0 }

    implicit def hlistDropAt[A <: HList, Pos <: Nat, R <: HList](implicit dropAt: hlists.DropAt.Aux[A, Pos, R]): Aux[A, Pos, R] =
      new DropAt[A, Pos] {
        type Out = R
      }

    implicit def tupleDropAt[A <: Product, AH <: HList, Pos <: Nat, R <: Product, RH <: HList](implicit
        genA: Generic.Aux[A, AH],
        genR: Generic.Aux[R, RH],
        dropAt: hlists.DropAt.Aux[AH, Pos, RH]): Aux[A, Pos, R] =
      new DropAt[A, Pos] {
        type Out = R
      }
  }

  trait JoinOnNat[L, R, PosL <: Nat, PosR <: Nat] {
    type Out
  }

  trait LowPriorityJoinOnNat {

    implicit def hlistTupleJoinOnNat[L <: HList, R <: Product, RH <: HList, PosL <: Nat, PosR <: Nat, Out0 <: HList](implicit
        gen: Generic.Aux[R, RH],
        joinOn: hlists.JoinOnNat.Aux[L, RH, PosL, PosR, Out0]): JoinOnNat.Aux[L, R, PosL, PosR, Out0] =
      new JoinOnNat[L, R, PosL, PosR] {
        type Out = Out0
      }

    implicit def tupleHListJoinOnNat[L <: Product, LH <: HList, R <: HList, RP <: Product, PosL <: Nat, PosR <: Nat, Out0 <: Product, Out0H <: HList](implicit
        genL: Generic.Aux[L, LH],
        tupler: Generic.Aux[R, RP],
        genOut0: Generic.Aux[Out0, Out0H],
        joinOn: hlists.JoinOnNat.Aux[LH, R, PosL, PosR, Out0H]): JoinOnNat.Aux[L, R, PosL, PosR, Out0] =
      new JoinOnNat[L, R, PosL, PosR] {
        type Out = Out0
      }
  }

  object JoinOnNat extends LowPriorityJoinOnNat {
    type Aux[L, R, PosL <: Nat, PosR <: Nat, Out0] = JoinOnNat[L, R, PosL, PosR] { type Out = Out0 }

    implicit def hlistJoinOnNat[L <: HList, R <: HList, PosL <: Nat, PosR <: Nat, Out0 <: HList](implicit
        joinOn: hlists.JoinOnNat.Aux[L, R, PosL, PosR, Out0]): Aux[L, R, PosL, PosR, Out0] =
      new JoinOnNat[L, R, PosL, PosR] {
        type Out = Out0
      }

    implicit def tupleJoinOnNat[L <: Product, LH <: HList, R <: Product, RH <: HList, PosL <: Nat, PosR <: Nat, Out0 <: Product, Out0H <: HList](implicit
        genL: Generic.Aux[L, LH],
        genR: Generic.Aux[R, RH],
        genOut0: Generic.Aux[Out0, Out0H],
        joinOn: hlists.JoinOnNat.Aux[LH, RH, PosL, PosR, Out0H]): Aux[L, R, PosL, PosR, Out0] =
      new JoinOnNat[L, R, PosL, PosR] {
        type Out = Out0
      }
  }

  trait Disjunct[A, B] {
    type Out
  }

  trait LowPriorityDisjunct {

    implicit def hlistTupleDisjunct[A <: HList, B <: Product, BH <: HList, R <: HList](implicit
        genB: Generic.Aux[B, BH],
        disjunct: hlists.Disjunct.Aux[A, BH, R]): Disjunct.Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }

    implicit def tupleHListDisjunct[A <: Product, AH <: HList, B <: HList, BP <: Product, R <: Product, RH <: HList](implicit
        genA: Generic.Aux[A, AH],
        tupler: Tupler.Aux[B, BP],
        genR: Generic.Aux[R, RH],
        disjunct: hlists.Disjunct.Aux[AH, B, RH]): Disjunct.Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }

  }

  object Disjunct extends LowPriorityDisjunct {
    type Aux[A, B, Out0] = Disjunct[A, B] { type Out = Out0 }

    implicit def hlistDisjunct[A <: HList, B <: HList, R <: HList](implicit
        disjunct: hlists.Disjunct.Aux[A, B, R]): Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }


    implicit def tupleDisjunct[A <: Product, AH <: HList, B <: Product, BH <: HList, R <: Product, RH <: HList](implicit
        genA: Generic.Aux[A, AH],
        genB: Generic.Aux[B, BH],
        genR: Generic.Aux[R, RH],
        disjunct: hlists.Disjunct.Aux[AH, BH, RH]): Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }
  }

  trait FromTraversable[Out] {
    def apply(l: GenTraversable[_]): Option[Out]
  }

  object FromTraversable {
    implicit def hlistFromTraversable[R <: HList](implicit
        fromTraversable: traversable.FromTraversable[R]): FromTraversable[R] =
      (l: GenTraversable[_]) => { fromTraversable(l) }

    implicit def tupleFromTraversable[P <: Product, R <: HList](implicit
        gen: Generic.Aux[P, R],
        fromTraversable: traversable.FromTraversable[R]): FromTraversable[P] =
      (l: GenTraversable[_]) => fromTraversable(l).map { hlist => gen.from(hlist) }

  }
}
