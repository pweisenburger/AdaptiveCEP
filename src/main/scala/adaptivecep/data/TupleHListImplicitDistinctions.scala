package adaptivecep.data

import shapeless.{Generic, HList, Nat}
import shapeless.ops._
import shapeless.ops.hlist.HKernelAux
import shapeless.ops.traversable.FromTraversable

import scala.collection.GenTraversable

// These type classes are need in order to support the usage of tuples and hlists within the dsl
trait LengthImplicit[T] {
  def apply(): Int
}

object LengthImplicit {

  implicit def hlistLength[T <: HList](implicit op: hlist.HKernelAux[T]): LengthImplicit[T] = new LengthImplicit[T] {
    override def apply(): Int = op().length
  }

  implicit def tupleLength[T <: Product, R <: HList](implicit gen: Generic.Aux[T, R], op: HKernelAux[R]): LengthImplicit[T] = new LengthImplicit[T] {
    override def apply(): Int = op().length
  }
}

trait PrependImplicit[A, B] {
  type Out
}

object PrependImplicit {

  type Aux[A, B, Out0] = PrependImplicit[A, B] { type Out = Out0 }

  implicit def hlistPrepend[A <: HList, B <: HList, R <: HList](implicit prepend: hlist.Prepend.Aux[A, B, R]): Aux[A, B, R] =
    new PrependImplicit[A, B] {
      type Out = R
    }

  implicit def tuplePrepend[A <: Product, B <: Product, R <: Product](implicit prepend: tuple.Prepend.Aux[A, B, R]): Aux[A, B, R] =
    new PrependImplicit[A, B] {
      type Out = R
    }
}

trait DropAtImplicit[A, Pos <: Nat] {
  type Out
}

object DropAtImplicit {
  type Aux[A, Pos <: Nat, Out0] = DropAtImplicit[A, Pos] { type Out = Out0 }

  implicit def hlistDropAt[A <: HList, Pos <: Nat, R <: HList](implicit dropAt: DropAt.Aux[A, Pos, R]): Aux[A, Pos, R] =
    new DropAtImplicit[A, Pos] {
      type Out = R
    }

  implicit def tupleDropAt[A <: Product, AH <: HList, Pos <: Nat, R <: Product, RH <: HList](implicit
      genA: Generic.Aux[A, AH],
      genR: Generic.Aux[R, RH],
      dropAt: DropAt.Aux[AH, Pos, RH]): Aux[A, Pos, R] =
    new DropAtImplicit[A, Pos] {
      type Out = R
    }
}

trait DisjunctImplicit[A, B] {
  type Out
}

object DisjunctImplicit {
  type Aux[A, B, Out0] = DisjunctImplicit[A, B] { type Out = Out0 }

  implicit def hlistDisjunct[A <: HList, B <: HList, R <: HList](implicit disjunct: Disjunct.Aux[A, B, R]): Aux[A, B, R] =
    new DisjunctImplicit[A, B] {
      type Out = R
    }

  implicit def tupleDisjunct[A <: Product, AH <: HList, B <: Product, BH <: HList, R <: Product, RH <: HList](implicit
      genA: Generic.Aux[A, AH],
      genB: Generic.Aux[B, BH],
      genR: Generic.Aux[R, RH],
      disjunct: Disjunct.Aux[AH, BH, RH]): Aux[A, B, R] =
    new DisjunctImplicit[A, B] {
      type Out = R
    }
}


trait FromTraversableImplicit[Out] {
  def apply(l : GenTraversable[_]) : Option[Out]
}

object FromTraversableImplicit {
  implicit def hlistFromTraversable[R <: HList](implicit fromTraversable: FromTraversable[R]): FromTraversableImplicit[R] =
    new FromTraversableImplicit[R] {
      override def apply(l: GenTraversable[_]): Option[R] = {
        fromTraversable(l)
      }
    }

  implicit def tupleFromTraversable[P <: Product, R <: HList](implicit gen: Generic.Aux[P, R], fromTraversable: FromTraversable[R]): FromTraversableImplicit[P] =
    new FromTraversableImplicit[P] {
      override def apply(l: GenTraversable[_]): Option[P] = fromTraversable(l).map { hlist => gen.from(hlist) }
    }

}
