/**********************************************************************************************************************/
/*                                                                                                                    */
/* EventScala                                                                                                         */
/*                                                                                                                    */
/* Developed by                                                                                                       */
/* Lucas Bärenfänger (@scalarookie)                                                                                   */
/*                                                                                                                    */
/* Visit scalarookie.com for more information.                                                                        */
/*                                                                                                                    */
/**********************************************************************************************************************/

package com.scalarookie.eventscala.tests

import org.scalatest.FunSuite
import com.scalarookie.eventscala.dsl._

case class A(id: Int)
case class B(id: Int)
case class C(id: Int)

@GetCompanionObject[A] object A
@GetCompanionObject[B] object B
@GetCompanionObject[C] object C

class DslTest extends FunSuite {

  test("Primitive representing a stream of instances of a class - No filter") {
    val d = select () {
      A()
    }
    val c = Select(Nil,
      Stream[A]("A", None))
    assert(d == c)
  }

  test("Primitive representing a stream of instances of a class - Filter 1") {
    val d = select () {
      A(A.id === 10)
    }
    val c = Select(Nil,
      Stream[A]("A", Some(Filter[A](Equals, Field[A, Int]("A", "id"), 10))))
    assert(d == c)
  }

  test("Primitive representing a stream of instances of a class - Filter 2") {
    val d = select () {
      A(A.id < 10)
    }
    val c = Select(Nil,
      Stream[A]("A", Some(Filter[A](Smaller, Field[A, Int]("A", "id"), 10))))
    assert(d == c)
  }

  test("Primitive representing a stream of instances of a class - Filter 3") {
    val d = select () {
      A(A.id > 10)
    }
    val c = Select(Nil,
      Stream[A]("A", Some(Filter[A](Greater, Field[A, Int]("A", "id"), 10))))
    assert(d == c)
  }

  test("Primitive representing a stream of instances of time - AtEvery") {
    val d = select () {
      AtEvery(** h ** m 30 s)
    }
    val c = Select(Nil,
      At(None, None, Some(30)))
    assert(d == c)
  }

  test("Primitive representing a stream of instances of time - AfterEvery") {
    val d = select () {
      AfterEvery(30 s)
    }
    val c = Select(Nil,
      After(30))
    assert(d == c)
  }

  test("Operator - Sequence") {
    val d = select () {
      A() -> B()
    }
    val c = Select(Nil,
      Sequence(Stream[A]("A", None), Stream[B]("B", None)))
    assert(d == c)
  }

  test("Operator - And") {
    val d = select () {
      A() & B()
    }
    val c = Select(Nil,
      And(Stream[A]("A", None), Stream[B]("B", None)))
    assert(d == c)
  }

  test("Operator - Or") {
    val d = select () {
      A() | B()
    }
    val c = Select(Nil,
      Or(Stream[A]("A", None), Stream[B]("B", None)))
    assert(d == c)
  }

  test("Operator - Not") {
    val d = select () {
      !A()
    }
    val c = Select(Nil,
      Not(Stream[A]("A", None)))
    assert(d == c)
  }

  test("Nested - 1") {
    val d = select (A.id, B.id, C.id) {
      A() -> (B() -> C())
    }
    val c = Select(List(Field[A, Int]("A", "id"), Field[B, Int]("B", "id"), Field[C, Int]("C", "id")),
      Sequence(Stream[A]("A", None), Sequence(Stream[B]("B", None), Stream[C]("C", None))))
    assert(d == c)
  }

  test("Nested - 2") {
    val d = select (A.id, B.id) {
      AfterEvery(3 s) -> (A() | B())
    }
    val c = Select(List(Field[A, Int]("A", "id"), Field[B, Int]("B", "id")),
      Sequence(After(3), Or(Stream[A]("A", None), Stream[B]("B", None))))
    assert(d == c)
  }

  test("Nested - 3") {
    val d = select (A.id, B.id) {
      AfterEvery(1 s) & !(A() | B())
    }
    val c = Select(List(Field[A, Int]("A", "id"), Field[B, Int]("B", "id")),
      And(After(1), Not(Or(Stream[A]("A", None), Stream[B]("B", None)))))
    assert(d == c)
  }

  test("Nested - 4") {
    val d = select (A.id, B.id) {
      AtEvery(** h ** m 15 s) -> (AfterEvery(1 s) & !A() | B())
    }
    val c = Select(List(Field[A, Int]("A", "id"), Field[B, Int]("B", "id")),
      Sequence(At(None, None, Some(15)), Or(And(After(1), Not(Stream[A]("A", None))), Stream[B]("B", None))))
    assert(d == c)
  }

}
