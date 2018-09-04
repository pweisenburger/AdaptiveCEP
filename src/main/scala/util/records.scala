package util

import shapeless.{DepFn2, HList}
import shapeless.ops.hlist.{Prepend, ToTraversable, ZipWithKeys}
import shapeless.ops.record.{Remover, Selector, UnzipFields, Values}
import shapeless.ops.traversable.FromTraversable

import scala.annotation.implicitNotFound
import scala.collection.GenTraversable

object records {

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

    implicit def defaultDropKey[L <: HList, K, V, R <: HList, Keys <: HList, Vals <: HList, AfterRemove <: HList](implicit
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
    * Type class supporting joining this `HList` and another `HList` based on the value of the key positions ''KeyL'' and ''KeyR''.
    *
    * @author André Pacak
    */
  trait JoinOnKey[L <: HList, R <: HList, KeyL, KeyR] extends DepFn2[L, R] with Serializable {
    type Out <: HList
  }

  object JoinOnKey {
    def apply[L <: HList, R <: HList, KeyL, KeyR](implicit joinOn: JoinOnKey[L, R, KeyL, KeyR]): Aux[L, R, KeyL, KeyR, joinOn.Out] = joinOn

    type Aux[L <: HList, R <: HList, KeyL, KeyR, Out0 <: HList] = JoinOnKey[L, R, KeyL, KeyR] {type Out = Out0}

    implicit def defaultJoinOn[L <: HList, R <: HList, KeyL, KeyR, On, V, Dropped <: HList, Out0 <: HList](implicit
        atL: Selector.Aux[L, KeyL, On],
        atR: Selector.Aux[R, KeyR, On],
        remove: Remover.Aux[R, KeyR, (V, Dropped)],
        prepend: Prepend.Aux[L, Dropped, Out0]
    ): Aux[L, R, KeyL, KeyR, Out0] = new JoinOnKey[L, R, KeyL, KeyR] {
      type Out = Out0

      def apply(t: L, u: R): Out = {
        val dropped = remove(u)
        prepend(t, dropped._2)
      }
    }
  }

  /**
    * Type class supporting getting the value for the key ''K'' of an extensible record.
    * Available only if this `HList` contains an element of shape FieldType[K, _].
    * This type class is used to select the value of a `GenTraversable[_]`.
    * It returns the value for the key as an Any object if successful.
    *
    * @author André Pacak
    */
  @implicitNotFound("Implicit not found: adaptivecep.data.SelectFromTraversable[${L}, ${K}]. You requested to select an element for the key ${K}, but the HList ${L} does not contain the key ${K}.")
  trait SelectFromTraversable[L <: HList, K] extends Serializable {
    def apply(l: GenTraversable[_]): Option[Any]
  }

  object SelectFromTraversable {
    def apply[L <: HList, K](implicit select: SelectFromTraversable[L, K]) = select

    implicit def defaultSelectFromTraversable[L <: HList, K, V, R <: HList, Keys <: HList, Vals <: HList, AfterRemove <: HList](implicit
        unzip: UnzipFields.Aux[L, Keys, Vals],
        fromVals: FromTraversable[Vals],
        zipWithKey: ZipWithKeys.Aux[Keys, Vals, L],
        selector: Selector[L, K]
    ): SelectFromTraversable[L, K] = new SelectFromTraversable[L, K] {
      override def apply(l: GenTraversable[_]): Option[Any] = fromVals(l).map { vals =>
        val record = zipWithKey(vals)
        selector(record).asInstanceOf[Any]
      }
    }
  }
}
