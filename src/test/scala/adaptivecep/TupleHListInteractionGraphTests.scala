package adaptivecep

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import akka.actor.ActorRef
import shapeless.{::, HNil, Nat}

class TupleHListInteractionGraphTests extends GraphTestSuite {

  test("Tuple/HList - SequenceNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[(String, String)]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(21, 42)
    Thread.sleep(2000)
    b ! Event("21", "42")
    expectMsg(Event(21, 42, "21", "42"))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - SequenceNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[(Int, Int, String, String)] =
      sequence(nStream[(Int, Int)]("A") -> nStream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(1, 1)
    Thread.sleep(2000)
    a ! Event(2, 2)
    Thread.sleep(2000)
    a ! Event(3, 3)
    Thread.sleep(2000)
    b ! Event("1", "1")
    Thread.sleep(2000)
    b ! Event("2", "2")
    Thread.sleep(2000)
    b ! Event("3", "3")
    expectMsg(Event(1, 1, "1", "1"))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - JoinNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: Query[(Int, Int)] = stream[(Int, Int)]("B")
    val query: Query[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
        .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("a", true, "b")
    a ! Event("c", true, "d")
    a ! Event("e", true, "f")
    a ! Event("g", true, "h")
    a ! Event("i", true, "j")
    Thread.sleep(2000)
    b ! Event(1, 2)
    b ! Event(3, 4)
    b ! Event(5, 6)
    b ! Event(7, 8)
    expectMsg(Event("a", true, "b", 1, 2))
    expectMsg(Event("c", true, "d", 1, 2))
    expectMsg(Event("e", true, "f", 1, 2))
    expectMsg(Event("a", true, "b", 3, 4))
    expectMsg(Event("c", true, "d", 3, 4))
    expectMsg(Event("e", true, "f", 3, 4))
    expectMsg(Event("a", true, "b", 5, 6))
    expectMsg(Event("c", true, "d", 5, 6))
    expectMsg(Event("e", true, "f", 5, 6))
    expectMsg(Event("a", true, "b", 7, 8))
    expectMsg(Event("c", true, "d", 7, 8))
    expectMsg(Event("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - JoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: Query[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: Query[(String, Boolean, String, Int, Int)] =
      stream[(String, Boolean, String)]("A")
        .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    b ! Event(1, 2)
    b ! Event(3, 4)
    b ! Event(5, 6)
    b ! Event(7, 8)
    Thread.sleep(2000)
    a ! Event("a", true, "b")
    a ! Event("c", true, "d")
    a ! Event("e", true, "f")
    a ! Event("g", true, "h")
    a ! Event("i", true, "j")
    expectMsg(Event("a", true, "b", 5, 6))
    expectMsg(Event("a", true, "b", 7, 8))
    expectMsg(Event("c", true, "d", 5, 6))
    expectMsg(Event("c", true, "d", 7, 8))
    expectMsg(Event("e", true, "f", 5, 6))
    expectMsg(Event("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  /*
  test("Tuple/HList - JoinOnNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: Query[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: Query[(String, Boolean, Int, Int)] =
      stream[(String, Boolean, Int)]("A")
        .joinOn(sq, Nat._3, Nat._1, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("a", true, 1)
    a ! Event("c", true, 2)
    a ! Event("e", true, 3)
    a ! Event("g", true, 4)
    a ! Event("i", true, 5)
    Thread.sleep(2000)
    b ! Event(1, 2)
    b ! Event(3, 4)
    b ! Event(5, 6)
    b ! Event(7, 8)
    expectMsg(Event("a", true, 1, 2))
    expectMsg(Event("e", true, 3, 4))
    stopActors(a, b, graph)
  }
  */

  test("Tuple/HList - JoinOnNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: Query[(Int, Int)] = stream[(Int, Int)]("B")
    val query: Query[String::Boolean::Int::Int::HNil] =
      stream[String::Boolean::Int::HNil]("A")
        .joinOn(sq, Nat._3, Nat._1, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("a", true, 1)
    a ! Event("c", true, 2)
    a ! Event("e", true, 3)
    a ! Event("g", true, 4)
    a ! Event("i", true, 5)
    Thread.sleep(2000)
    b ! Event(1, 2)
    b ! Event(3, 4)
    b ! Event(5, 6)
    b ! Event(7, 8)
    expectMsg(Event("a", true, 1, 2))
    expectMsg(Event("e", true, 3, 4))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - ConjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[(Int, Float)] =
      stream[Tuple1[Int]]("A")
        .and(stream[Float::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(21)
    b ! Event(21.0f)
    Thread.sleep(2000)
    a ! Event(42)
    b ! Event(42.0f)
    expectMsg(Event(21, 21.0f))
    expectMsg(Event(42, 42.0f))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - ConjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Int::Float::HNil] =
      stream[Int::HNil]("A")
        .and(stream[Tuple1[Float]]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(21)
    a ! Event(42)
    Thread.sleep(2000)
    b ! Event(21.0f)
    b ! Event(42.0f)
    expectMsg(Event(21, 21.0f))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - DisjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[(Either[Int, String], Either[Int, String])] =
      stream[(Int, Int)]("A")
        .or(stream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(21, 42)
    Thread.sleep(2000)
    b ! Event("21", "42")
    expectMsg(Event(Left(21), Left(42)))
    expectMsg(Event(Right("21"), Right("42")))
    stopActors(a, b, graph)
  }

  test("Tuple/HList - DisjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val query: Query[Either[Either[Int, String], Boolean]:: Either[Either[Int, String], Boolean]:: Either[Unit,Boolean]::HNil] =
      stream[Int::Int::HNil]("A")
        .or(stream[(String, String)]("B"))
        .or(stream[Boolean::Boolean::Boolean::HNil]("C"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    a ! Event(21, 42)
    Thread.sleep(2000)
    b ! Event("21", "42")
    Thread.sleep(2000)
    c ! Event(true, false, true)
    expectMsg(Event(Left(Left(21)), Left(Left(42)), Left(())))
    expectMsg(Event(Left(Right("21")), Left(Right("42")), Left(())))
    expectMsg(Event(Right(true), Right(false), Right(true)))
    stopActors(a, b, c, graph)
  }
}
