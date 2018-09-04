package adaptivecep.data

import shapeless.{Generic, HList, Nat}
import shapeless.ops._
import shapeless.ops.hlist.HKernelAux

// These type classes are need in order to support the usage of tuples and hlists within the dsl
trait LengthImplicit[T] {
  def length: Int
}

object LengthImplicit {

  implicit def hlistLength[T <: HList](implicit op: hlist.HKernelAux[T]): LengthImplicit[T] = new LengthImplicit[T] {
    override def length: Int = op().length
  }

  implicit def tupleLength[T <: Product, R <: HList](implicit gen: Generic.Aux[T, R], op: HKernelAux[R]): LengthImplicit[T] = new LengthImplicit[T] {
    val length: Int = op().length
  }
}

trait PrependImplicit[A, B] {
  type Out
}

object PrependImplicit {

  type Aux[A, B, Out0] = PrependImplicit[A, B] { type Out = Out0 }

  implicit def hlistPreprend[A <: HList, B <: HList, R <: HList](implicit prepend: hlist.Prepend.Aux[A, B, R]): Aux[A, B, R] =
    new PrependImplicit[A, B] {
      type Out = R
    }

  implicit def hlistPreprend[A <: Product, B <: Product, R <: Product](implicit prepend: tuple.Prepend.Aux[A, B, R]): Aux[A, B, R] =
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
}

