package util

import shapeless.ops.hlist.{HKernelAux, Tupler}
import shapeless.ops.{hlist, traversable, tuple}
import shapeless.{::, Generic, HList, HNil, Nat, Succ}

import scala.collection.GenTraversable

// These type classes are need in order to support the usage of tuples and hlists within the dsl
object tuplehlistsupport {

  trait Length[T] {
    def apply(): Int
  }

  trait LowPriorityLength {

    implicit def singleLength[T]: Length[T] = () => 1
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

  trait LowPriorityPrepend0 {

    implicit def singleSinglePrepend[A, B]: Prepend.Aux[A, B, Tuple2[A, B]] =
      new Prepend[A, B] {
        type Out = Tuple2[A, B]
      }
  }

  trait LowPriorityPrepend1 extends LowPriorityPrepend0 {

    implicit def singleTuplePrepend[A, B <: Product, R <: Product](implicit
        prepend: tuple.Prepend.Aux[Tuple1[A], B, R]): Prepend.Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }

    implicit def singleHListPrepend[A, B <: HList, R <: HList](implicit
        prepend: hlist.Prepend.Aux[A::HNil, B, R]): Prepend.Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }

    implicit def tupleSinglePrepend[A <: Product , B, R <: Product](implicit
        prepend: tuple.Prepend.Aux[A, Tuple1[B], R]): Prepend.Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }

    implicit def hlistSinglePrepend[A <: HList, B, R <: HList](implicit
        prepend: hlist.Prepend.Aux[A, B::HNil, R]): Prepend.Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }
  }

  trait LowPriorityPrepend2 extends LowPriorityPrepend1 {

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

  object Prepend extends LowPriorityPrepend2 {
    type Aux[A, B, Out0] = Prepend[A, B] { type Out = Out0 }

    implicit def hlistPrepend[A <: HList, B <: HList, R <: HList](implicit
        prepend: hlist.Prepend.Aux[A, B, R]): Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }

    implicit def tuplePrepend[A <: Product, B <: Product, R <: Product](implicit
        prepend: tuple.Prepend.Aux[A, B, R]): Aux[A, B, R] =
      new Prepend[A, B] {
        type Out = R
      }
  }

  trait DropAt[A, Pos <: Nat] {
    type Out
  }

  trait LowPriorityDropAt {
    // need to be HNil because there is no Tuple0
    implicit def singleDropAt[A]: DropAt.Aux[A, Succ[Nat._0], HNil] =
      new DropAt[A, Succ[Nat._0]] {
        type Out = HNil
      }
  }

  object DropAt extends LowPriorityDropAt {
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

  trait LowPriorityJoinOnNat0 {

    implicit def singleSingleJoinOnNat[L, R](implicit
        equal: L =:= R): JoinOnNat.Aux[L, R, Succ[Nat._0], Succ[Nat._0], L] =
      new JoinOnNat[L, R, Succ[Nat._0], Succ[Nat._0]] {
        type Out = L
      }
  }

  trait LowPriorityJoinOnNat1 extends LowPriorityJoinOnNat0 {

    implicit def singleTupleJoinOnNat[L, R <: Product, RH <: HList, PosR <: Nat, Out0 <: Product, Out0H <: HList](implicit
        genR: Generic.Aux[R, RH],
        tuplerOut0: Tupler.Aux[Out0H, Out0],
        joinOnNat: hlists.JoinOnNat.Aux[L::HNil, RH, Succ[Nat._0], PosR, Out0H]
    ): JoinOnNat.Aux[L, R, Succ[Nat._0], PosR, Out0] =
      new JoinOnNat[L, R, Succ[Nat._0], PosR] {
        type Out = Out0
      }

    implicit def singleHListJoinOnNat[L, R <: HList, PosR <: Nat, Out0 <: HList](implicit
        joinOnNat: hlists.JoinOnNat.Aux[L::HNil, R, Succ[Nat._0], PosR, Out0]
    ): JoinOnNat.Aux[L, R, Succ[Nat._0], PosR, Out0] =
      new JoinOnNat[L, R, Succ[Nat._0], PosR] {
        type Out = Out0
      }

    implicit def tupleSingleJoinOnNat[L <: Product, R, LH <: HList, PosL <: Nat, Out0 <: Product, Out0H <: HList](implicit
        genL: Generic.Aux[L, LH],
        tuplerOut0: Tupler.Aux[Out0H, Out0],
        joinOnNat: hlists.JoinOnNat.Aux[LH, R::HNil, PosL, Succ[Nat._0], Out0H]
    ): JoinOnNat.Aux[L, R, PosL, Succ[Nat._0], Out0] =
      new JoinOnNat[L, R, PosL, Succ[Nat._0]] {
        type Out = Out0
      }

    implicit def hlistSingleJoinOnNat[L <: HList, R, PosL <: Nat, Out0 <: HList](implicit
        joinOnNat: hlists.JoinOnNat.Aux[L, R::HNil, PosL, Succ[Nat._1], Out0]
    ): JoinOnNat.Aux[L, R, PosL, Succ[Nat._0], Out0] =
      new JoinOnNat[L, R, PosL, Succ[Nat._0]] {
        type Out = Out0
      }
  }

  trait LowPriorityJoinOnNat2 extends LowPriorityJoinOnNat1 {

    implicit def hlistTupleJoinOnNat[L <: HList, R <: Product, RH <: HList, PosL <: Nat, PosR <: Nat, Out0 <: HList](implicit
        gen: Generic.Aux[R, RH],
        joinOn: hlists.JoinOnNat.Aux[L, RH, PosL, PosR, Out0]): JoinOnNat.Aux[L, R, PosL, PosR, Out0] =
      new JoinOnNat[L, R, PosL, PosR] {
        type Out = Out0
      }

    implicit def tupleHListJoinOnNat[L <: Product, LH <: HList, R <: HList, PosL <: Nat, PosR <: Nat, Out0 <: Product, Out0H <: HList](implicit
        genL: Generic.Aux[L, LH],
        genOut0: Generic.Aux[Out0, Out0H],
        joinOn: hlists.JoinOnNat.Aux[LH, R, PosL, PosR, Out0H]): JoinOnNat.Aux[L, R, PosL, PosR, Out0] =
      new JoinOnNat[L, R, PosL, PosR] {
        type Out = Out0
      }
  }

  object JoinOnNat extends LowPriorityJoinOnNat2 {
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

  trait LowPriorityDisjunct0 {

    implicit def singleSingleDisjunct[A, B]: Disjunct.Aux[A, B, Tuple1[Either[A, B]]] =
      new Disjunct[A, B] {
        type Out = Tuple1[Either[A, B]]
      }
  }

  trait LowPriorityDisjunct1 extends LowPriorityDisjunct0 {

    implicit def singleTupleDisjunct[A, B <: Product, BH <: HList, RH <: HList, R <: Product](implicit
        genB: Generic.Aux[B, BH],
        disjunct: hlists.Disjunct.Aux[A::HNil, BH, RH]): Disjunct.Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }

    implicit def singleHListDisjunct[A, B <: HList, R <: HList](implicit
        disjunct: hlists.Disjunct.Aux[A::HNil, B, R]): Disjunct.Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }

    implicit def tupleSingleDisjunct[A <: Product, B, AH <: HList, RH <: HList, R <: Product](implicit
        genA: Generic.Aux[A, AH],
        disjunct: hlists.Disjunct.Aux[AH, B::HNil, RH]): Disjunct.Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }

    implicit def hlistSingleDisjunct[A <: HList, B, R <: HList](implicit
        disjunct: hlists.Disjunct.Aux[A, B::HNil, R]): Disjunct.Aux[A, B, R] =
      new Disjunct[A, B] {
        type Out = R
      }
  }

  trait LowPriorityDisjunct2 extends LowPriorityDisjunct1 {

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

  object Disjunct extends LowPriorityDisjunct2 {
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
