package adaptivecep.data

import shapeless.ops.hlist.{At, Patcher, Prepend, ToTraversable, ZipWithKeys}
import shapeless.ops.nat.Pred
import shapeless.ops.record.{Remover, UnzipFields, Values}
import shapeless.ops.traversable.FromTraversable
import shapeless.{::, DepFn1, DepFn2, HList, HNil, Nat}

import scala.annotation.implicitNotFound
import scala.collection.GenTraversable

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
    (implicit pred: Pred.Aux[N, Pred], patch: Patcher.Aux[Pred, Nat._1, L, HNil, R]): Aux[L, N, R] =
      new DropAt[L, N] {
        type Out = patch.Out
        override def apply(l: L): Out = { patch(l, HNil) }
      }
}

/**
  * Type class supporting dropping the key ''K'' of an extensible record.
  * Available only if this `HList` contains an element of shape FieldType[K, _].
  * This type class is used to transform a `GenTraversable[_]` and drops the value of the key for the extensible record.
  * It returns the altered extensible record as a Seq[Any] object if successful.
  * @author André Pacak
  */
@implicitNotFound("Implicit not found: adaptivecep.data.DropKey[${L}, ${K}]. You requested to drop an element for the key ${K}, but the HList ${L} does not contain the key ${K}.")
trait DropKey[L <: HList, K] extends Serializable {
  def apply(l: GenTraversable[_]): Option[Seq[Any]]
}

object DropKey {
  def apply[L <: HList, K](implicit dropKey: DropKey[L, K]) = dropKey

  implicit def defaultDropKey[L <: HList, K, V, R <: HList, Keys <: HList, Vals <: HList, AfterRemove <: HList]
    (implicit
      remover: Remover.Aux[L, K, (V, R)],
      unzip: UnzipFields.Aux[L, Keys, Vals],
      fromVals: FromTraversable[Vals],
      zipWithKey: ZipWithKeys.Aux[Keys, Vals, L],
      valuesAfterRemove: Values.Aux[R, AfterRemove],
      toTraversable: ToTraversable[AfterRemove, Seq]
    ): DropKey[L, K] = new DropKey[L, K] {
      override def apply(l: GenTraversable[_]): Option[Seq[Any]] = fromVals(l).map { vals =>
        val record = zipWithKey(vals)
        val removed = remover(record)
        val values = valuesAfterRemove(removed._2)
        toTraversable(values)
      }
    }
}

/**
  * Type class supporting the disjunction of this `HList` and another. It results in a `HList` that creates for each pair of (L,T) an Either[L, R].
  * If one list is shorter than the other the type that is used is Unit.
  * This type class was inspired by ZipWith.
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
