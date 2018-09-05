package adaptivecep

import adaptivecep.data.Events.{Created, Event}
import adaptivecep.data.Queries.Query
import adaptivecep.dsl.Dsl._
import akka.actor.ActorRef
import shapeless.{::, HNil, Nat}

class EmptyQueryGraphTests extends GraphTestSuite {

  test("Empty Query - Simple") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query[HNil] =
      stream[HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event()
    a ! Event()
    expectMsg(Event())
    expectMsg(Event())
    stopActors(a, graph)
  }

  test("Empty Query - DropNode") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query[HNil] = stream[Int::HNil]("A").drop(Nat._1)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event()
    expectMsg(Event())
    stopActors(a, graph)
  }

  test("Empty Query - SelfJoinNode") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query[HNil] =
      stream[HNil]("A")
        .selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event()
    a ! Event()
    a ! Event()
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    stopActors(a, graph)
  }

  test("Empty Query - Join - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[HNil] = stream[HNil]("A")
      .join(stream[HNil]("B"), tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event()
    a ! Event()
    a ! Event()
    a ! Event()
    a ! Event()
    Thread.sleep(2000)
    b ! Event()
    b ! Event()
    b ! Event()
    b ! Event()
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    expectMsg(Event())
    stopActors(a, b, graph)
  }

  test("Empty Query - Join - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Int::HNil] = stream[Int::HNil]("A")
      .join(stream[HNil]("B"), tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(1)
    a ! Event(2)
    a ! Event(3)
    a ! Event(4)
    a ! Event(5)
    Thread.sleep(2000)
    b ! Event()
    b ! Event()
    b ! Event()
    b ! Event()
    expectMsg(Event(1))
    expectMsg(Event(2))
    expectMsg(Event(3))
    expectMsg(Event(1))
    expectMsg(Event(2))
    expectMsg(Event(3))
    expectMsg(Event(1))
    expectMsg(Event(2))
    expectMsg(Event(3))
    expectMsg(Event(1))
    expectMsg(Event(2))
    expectMsg(Event(3))
    stopActors(a, b, graph)
  }

  test("Empty Query - Join - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Int::HNil] = stream[HNil]("A")
      .join(stream[Int::HNil]("B"), tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event()
    a ! Event()
    a ! Event()
    a ! Event()
    a ! Event()
    Thread.sleep(2000)
    b ! Event(1)
    b ! Event(2)
    b ! Event(3)
    b ! Event(4)
    expectMsg(Event(1))
    expectMsg(Event(1))
    expectMsg(Event(1))
    expectMsg(Event(2))
    expectMsg(Event(2))
    expectMsg(Event(2))
    expectMsg(Event(3))
    expectMsg(Event(3))
    expectMsg(Event(3))
    expectMsg(Event(4))
    expectMsg(Event(4))
    expectMsg(Event(4))
    stopActors(a, b, graph)
  }

  test("Empty Query - ConjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[HNil] =
      stream[HNil]("A")
        .and(stream[HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event()
    b ! Event()
    Thread.sleep(2000)
    a ! Event()
    b ! Event()
    expectMsg(Event())
    expectMsg(Event())
    stopActors(a, b, graph)
  }

  test("Empty Query - ConjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Int::HNil] =
      stream[Int::HNil]("A")
        .and(stream[HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(1)
    b ! Event()
    Thread.sleep(2000)
    a ! Event(2)
    b ! Event()
    expectMsg(Event(1))
    expectMsg(Event(2))
    stopActors(a, b, graph)
  }

  test("Empty Query - ConjunctionNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Boolean::HNil] =
      stream[HNil]("A")
        .and(stream[Boolean::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event()
    b ! Event(true)
    Thread.sleep(2000)
    a ! Event()
    b ! Event(false)
    expectMsg(Event(true))
    expectMsg(Event(false))
    stopActors(a, b, graph)
  }

  test("Empty Query - DisjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[HNil] =
      stream[HNil]("A")
        .or(stream[HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event()
    b ! Event()
    expectMsg(Event())
    expectMsg(Event())
    stopActors(a, b, graph)
  }

  test("Empty Query - DisjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Either[Int, Unit]::HNil] =
      stream[Int::HNil]("A")
        .or(stream[HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(21)
    Thread.sleep(2000)
    b ! Event()
    expectMsg(Event(Left(21)))
    expectMsg(Event())
    stopActors(a, b, graph)
  }

  test("Empty Query - DisjunctionNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query[Either[Unit, Int]::HNil] =
      stream[HNil]("A")
        .or(stream[Int::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event()
    expectMsg(Event())
    b ! Event(21)
    expectMsg(Event(Right(21)))
    stopActors(a, b, graph)
  }
}
