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
import com.scalarookie.eventscala.backend.esper._

class BackendEsperTests extends FunSuite {

  test("Primitive representing a stream of instances of a class - No filter") {
    val c = Select(Nil,
      Stream[A]("A", None))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every A=A]")
  }

  test("Primitive representing a stream of instances of a class - Filter 1") {
    val c = Select(Nil,
      Stream[A]("A", Some(Filter[A](Equals, Field[A, Int]("A", "id"), 10))))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every A=A(id = 10)]")
  }

  test("Primitive representing a stream of instances of a class - Filter 2") {
    val c = Select(Nil,
      Stream[A]("A", Some(Filter[A](Smaller, Field[A, Int]("A", "id"), 10))))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every A=A(id < 10)]")
  }

  test("Primitive representing a stream of instances of a class - Filter 3") {
    val c = Select(Nil,
      Stream[A]("A", Some(Filter[A](Greater, Field[A, Int]("A", "id"), 10))))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every A=A(id > 10)]")
  }

  test("Primitive representing a stream of instances of time - At") {
    val c = Select(Nil,
      At(None, None, Some(30)))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every timer:at(*, *, *, *, *, 30, *)]")
  }

  test("Primitive representing a stream of instances of time - After") {
    val c = Select(Nil,
      After(30))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every timer:interval(30 sec)]")
  }

  test("Operator - Sequence") {
    val c = Select(Nil,
      Sequence(Stream[A]("A", None), Stream[B]("B", None)))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every (A=A -> B=B)]")
  }

  test("Operator - And") {
    val c = Select(Nil,
      And(Stream[A]("A", None), Stream[B]("B", None)))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every (A=A and B=B)]")
  }

  test("Operator - Or") {
    val c = Select(Nil,
      Or(Stream[A]("A", None), Stream[B]("B", None)))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every (A=A or B=B)]")
  }

  test("Operator - Not") {
    val c = Select(Nil,
      Not(Stream[A]("A", None)))
    assert(query2EplString(c) ==
      "select * from " +
        "pattern [every (not A=A)]")
  }

  test("Nested - 1") {
    val c = Select(List(Field[A, Int]("A", "id"), Field[A, Int]("B", "id"), Field[A, Int]("C", "id")),
      Sequence(Stream[A]("A", None), Sequence(Stream[B]("B", None), Stream[C]("C", None))))
    assert(query2EplString(c) ==
      "select A.id, B.id, C.id from " +
        "pattern [every (A=A -> (B=B -> C=C))]")
  }

  test("Nested - 2") {
    val c = Select(List(Field[A, Int]("A", "id"), Field[A, Int]("B", "id")),
      Sequence(After(3), Or(Stream[A]("A", None), Stream[B]("B", None))))
    assert(query2EplString(c) ==
      "select A.id, B.id from " +
        "pattern [every (timer:interval(3 sec) -> (A=A or B=B))]")
  }

  test("Nested - 3") {
    val c = Select(List(Field[A, Int]("A", "id"), Field[A, Int]("B", "id")),
      And(After(1), Not(Or(Stream[A]("A", None), Stream[B]("B", None)))))
    assert(query2EplString(c) ==
      "select A.id, B.id from " +
        "pattern [every (timer:interval(1 sec) and (not (A=A or B=B)))]")
  }

  test("Nested - 4") {
    val c = Select(List(Field[A, Int]("A", "id"), Field[A, Int]("B", "id")),
      Sequence(At(None, None, Some(15)), Or(And(After(1), Not(Stream[A]("A", None))), Stream[B]("B", None))))
    assert(query2EplString(c) ==
      "select A.id, B.id from " +
        "pattern [every (timer:at(*, *, *, *, *, 15, *) -> ((timer:interval(1 sec) and (not A=A)) or B=B))]")
  }

}
