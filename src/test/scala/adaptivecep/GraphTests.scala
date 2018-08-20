package adaptivecep

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.factory._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import shapeless.{::, HNil, Nat}

class GraphTests extends TestKit(ActorSystem()) with FunSuiteLike with BeforeAndAfterAll {

  def createTestPublisher(name: String): ActorRef = {
    system.actorOf(Props(TestPublisher()), name)
  }

  def createTestGraph(query: Query, publishers: Map[String, ActorRef], testActor: ActorRef): ActorRef = GraphFactory.createImpl(
    system,
    query,
    publishers,
    DummyMonitorFactory(),
    DummyMonitorFactory(),
    () => testActor ! Created,
    event => testActor ! event)

  // The following method implementation is taken straight out of the Akka docs:
  // http://doc.akka.io/docs/akka/current/scala/testing.html#Watching_Other_Actors_from_Probes
  def stopActor(actor: ActorRef): Unit = {
    val probe = TestProbe()
    probe watch actor
    actor ! PoisonPill
    probe.expectTerminated(actor)
  }

  def stopActors(actors: ActorRef*): Unit = {
    actors.foreach(stopActor)
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  test("LeafNode - StreamNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::HNil] = stream[String::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList("42")
    expectMsg(EventList("42"))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(42, 42)
    expectMsg(EventList(42, 42))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Long::Long::Long::HNil] = stream[Long::Long::Long::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(42l, 42l, 42l)
    expectMsg(EventList(42l, 42l, 42l))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Float::Float::Float::Float::HNil] = stream[Float::Float::Float::Float::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(42f, 42f, 42f, 42f)
    expectMsg(EventList(42f, 42f, 42f, 42f))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Double::Double::Double::Double::Double::HNil] =
      stream[Double::Double::Double::Double::Double::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(42.0, 42.0, 42.0, 42.0, 42.0)
    expectMsg(EventList(42.0, 42.0, 42.0, 42.0, 42.0))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Boolean::Boolean::Boolean::Boolean::Boolean::Boolean::HNil] =
      stream[Boolean::Boolean::Boolean::Boolean::Boolean::Boolean::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(true, true, true, true, true, true)
    expectMsg(EventList(true, true, true, true, true, true))
    stopActors(a, graph)
  }


  test("LeafNode - SequenceNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList(21, 42)
    Thread.sleep(2000)
    b ! EventList("21", "42")
    expectMsg(EventList(21, 42, "21", "42"))
    stopActors(a, b, graph)
  }

  test("LeafNode - SequenceNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList(1, 1)
    Thread.sleep(2000)
    a ! EventList(2, 2)
    Thread.sleep(2000)
    a ! EventList(3, 3)
    Thread.sleep(2000)
    b ! EventList("1", "1")
    Thread.sleep(2000)
    b ! EventList("2", "2")
    Thread.sleep(2000)
    b ! EventList("3", "3")
    expectMsg(EventList(1, 1, "1", "1"))
    stopActors(a, b, graph)
  }

  test("UnaryNode - FilterNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("A")
      .where(_ >= _)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(41, 42)
    a ! EventList(42, 42)
    a ! EventList(43, 42)
    expectMsg(EventList(42, 42))
    expectMsg(EventList(43, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("A")
      .where(_ <= _)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(41, 42)
    a ! EventList(42, 42)
    a ! EventList(43, 42)
    expectMsg(EventList(41, 42))
    expectMsg(EventList(42, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Long::HNil] =
      stream[Long::HNil]("A")
      .where(_ == 42l)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(41l)
    a ! EventList(42l)
    expectMsg(EventList(42l))
    stopActors(a, graph)
  }
  test("UnaryNode - FilterNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Float::HNil] =
      stream[Float::HNil]("A")
      .where(_ > 41f)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(41f)
    a ! EventList(42f)
    expectMsg(EventList(42f))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Double::HNil] =
      stream[Double::HNil]("A")
      .where(_ < 42.0)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(41.0)
    a ! EventList(42.0)
    expectMsg(EventList(41.0))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query =
      stream[Boolean::HNil]("A")
      .where(_ != true)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(true)
    a ! EventList(false)
    expectMsg(EventList(false))
    stopActors(a, graph)
  }

  test("UnaryNode - DropElemNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::HNil] =
      stream[Int::Int::HNil]("A")
        .drop(Nat._2)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList(21, 42)
    a ! EventList(42, 21)
    expectMsg(EventList(21))
    expectMsg(EventList(42))
    stopActors(a, graph)
  }

  test("UnaryNode - DropElemNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::String::HNil] =
      stream[String::String::String::String::HNil]("A")
      .drop(Nat._1)
      .drop(Nat._2)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList("a", "b", "c", "d")
    a ! EventList("e", "f", "g", "h")
    expectMsg(EventList("b", "d"))
    expectMsg(EventList("f", "h"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::String::String::String::HNil] =
      stream[String::String::HNil]("A")
      .selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList("a", "b")
    a ! EventList("c", "d")
    a ! EventList("e", "f")
    expectMsg(EventList("a", "b", "a", "b"))
    expectMsg(EventList("a", "b", "c", "d"))
    expectMsg(EventList("c", "d", "a", "b"))
    expectMsg(EventList("c", "d", "c", "d"))
    expectMsg(EventList("e", "f", "a", "b"))
    expectMsg(EventList("e", "f", "c", "d"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::String::String::String::HNil] =
      stream[String::String::HNil]("A")
      .selfJoin(slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! EventList("a", "b")
    a ! EventList("c", "d")
    a ! EventList("e", "f")
    expectMsg(EventList("a", "b", "a", "b"))
    expectMsg(EventList("c", "d", "a", "b"))
    expectMsg(EventList("c", "d", "c", "d"))
    expectMsg(EventList("a", "b", "c", "d"))
    expectMsg(EventList("e", "f", "c", "d"))
    expectMsg(EventList("e", "f", "e", "f"))
    expectMsg(EventList("a", "b", "e", "f"))
    expectMsg(EventList("c", "d", "e", "f"))
    stopActors(a, graph)
  }

  test("BinaryNode - JoinNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList("a", true, "b")
    a ! EventList("c", true, "d")
    a ! EventList("e", true, "f")
    a ! EventList("g", true, "h")
    a ! EventList("i", true, "j")
    Thread.sleep(2000)
    b ! EventList(1, 2)
    b ! EventList(3, 4)
    b ! EventList(5, 6)
    b ! EventList(7, 8)
    expectMsg(EventList("a", true, "b", 1, 2))
    expectMsg(EventList("c", true, "d", 1, 2))
    expectMsg(EventList("e", true, "f", 1, 2))
    expectMsg(EventList("a", true, "b", 3, 4))
    expectMsg(EventList("c", true, "d", 3, 4))
    expectMsg(EventList("e", true, "f", 3, 4))
    expectMsg(EventList("a", true, "b", 5, 6))
    expectMsg(EventList("c", true, "d", 5, 6))
    expectMsg(EventList("e", true, "f", 5, 6))
    expectMsg(EventList("a", true, "b", 7, 8))
    expectMsg(EventList("c", true, "d", 7, 8))
    expectMsg(EventList("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    b ! EventList(1, 2)
    b ! EventList(3, 4)
    b ! EventList(5, 6)
    b ! EventList(7, 8)
    Thread.sleep(2000)
    a ! EventList("a", true, "b")
    a ! EventList("c", true, "d")
    a ! EventList("e", true, "f")
    a ! EventList("g", true, "h")
    a ! EventList("i", true, "j")
    expectMsg(EventList("a", true, "b", 5, 6))
    expectMsg(EventList("a", true, "b", 7, 8))
    expectMsg(EventList("c", true, "d", 5, 6))
    expectMsg(EventList("c", true, "d", 7, 8))
    expectMsg(EventList("e", true, "f", 5, 6))
    expectMsg(EventList("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test ("BinaryNode - JoinNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList("a", true, "b")
    a ! EventList("c", true, "d")
    a ! EventList("e", true, "f")
    a ! EventList("g", true, "h")
    a ! EventList("i", true, "j")
    Thread.sleep(2000)
    b ! EventList(1, 2)
    b ! EventList(3, 4)
    b ! EventList(5, 6)
    b ! EventList(7, 8)
    expectMsg(EventList("e", true, "f", 1, 2))
    expectMsg(EventList("g", true, "h", 1, 2))
    expectMsg(EventList("i", true, "j", 1, 2))
    expectMsg(EventList("e", true, "f", 3, 4))
    expectMsg(EventList("g", true, "h", 3, 4))
    expectMsg(EventList("i", true, "j", 3, 4))
    expectMsg(EventList("e", true, "f", 5, 6))
    expectMsg(EventList("g", true, "h", 5, 6))
    expectMsg(EventList("i", true, "j", 5, 6))
    expectMsg(EventList("e", true, "f", 7, 8))
    expectMsg(EventList("g", true, "h", 7, 8))
    expectMsg(EventList("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    b ! EventList(1, 2)
    b ! EventList(3, 4)
    b ! EventList(5, 6)
    b ! EventList(7, 8)
    Thread.sleep(2000)
    a ! EventList("a", true, "b")
    a ! EventList("c", true, "d")
    a ! EventList("e", true, "f")
    a ! EventList("g", true, "h")
    a ! EventList("i", true, "j")
    expectMsg(EventList("a", true, "b", 5, 6))
    expectMsg(EventList("a", true, "b", 7, 8))
    expectMsg(EventList("c", true, "d", 5, 6))
    expectMsg(EventList("c", true, "d", 7, 8))
    expectMsg(EventList("e", true, "f", 5, 6))
    expectMsg(EventList("e", true, "f", 7, 8))
    expectMsg(EventList("g", true, "h", 5, 6))
    expectMsg(EventList("g", true, "h", 7, 8))
    expectMsg(EventList("i", true, "j", 5, 6))
    expectMsg(EventList("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("Binary Node - ConjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Float::HNil] =
      stream[Int::HNil]("A")
      .and(stream[Float::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList(21)
    b ! EventList(21.0f)
    Thread.sleep(2000)
    a ! EventList(42)
    b ! EventList(42.0f)
    expectMsg(EventList(21, 21.0f))
    expectMsg(EventList(42, 42.0f))
    stopActors(a, b, graph)
  }

  test("Binary Node - ConjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Float::HNil] =
      stream[Int::HNil]("A")
      .and(stream[Float::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList(21)
    a ! EventList(42)
    Thread.sleep(2000)
    b ! EventList(21.0f)
    b ! EventList(42.0f)
    expectMsg(EventList(21, 21.0f))
    stopActors(a, b, graph)
  }

  test("Binary Node - DisjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Either[Int, String]::Either[Int, String]::HNil] =
      stream[Int::Int::HNil]("A")
      .or(stream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! EventList(21, 42)
    Thread.sleep(2000)
    b ! EventList("21", "42")
    expectMsg(EventList(Left(21), Left(42)))
    expectMsg(EventList(Right("21"), Right("42")))
    stopActors(a, b, graph)
  }

  test("Binary Node - DisjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val query: HListQuery[Either[Either[Int, String], Boolean]:: Either[Either[Int, String], Boolean]:: Either[Unit,Boolean]::HNil] =
      stream[Int::Int::HNil]("A")
        .or(stream[String::String::HNil]("B"))
        .or(stream[Boolean::Boolean::Boolean::HNil]("C"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    a ! EventList(21, 42)
    Thread.sleep(2000)
    b ! EventList("21", "42")
    Thread.sleep(2000)
    c ! EventList(true, false, true)
    expectMsg(EventList(Left(Left(21)), Left(Left(42)), Left(())))
    expectMsg(EventList(Left(Right("21")), Left(Right("42")), Left(())))
    expectMsg(EventList(Right(true), Right(false), Right(true)))
    stopActors(a, b, c, graph)
  }

  test("Nested - SP operators") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val sq1: HListQuery[String::String::HNil] =
      stream[String::String::HNil]("A")
    val sq2: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("B")
    val sq3: HListQuery[String::HNil] =
      stream[String::HNil]("C")
    val sq4: HListQuery[String::String::Int::Int::HNil] =
      sq1.join(sq2, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val sq5: HListQuery[String::String::HNil] =
      sq3.selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val sq6: HListQuery[String::String::Int::Int::String::String::HNil] =
      sq4.join(sq5, tumblingWindow(1.instances), tumblingWindow(4.instances))
    val sq7: HListQuery[String::String::Int::Int::String::String::HNil] =
      sq6.where((_, _, e3, e4, _, _) => e3 < e4)
    val query: HListQuery[String::String::HNil] =
      sq7
        .drop(Nat._2)
        .drop(Nat._2)
        .drop(Nat._2)
        .drop(Nat._2)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    b ! EventList(1, 2)
    b ! EventList(3, 4)
    b ! EventList(5, 6)
    b ! EventList(7, 8)
    Thread.sleep(2000)
    a ! EventList("a", "b")
    a ! EventList("c", "d")
    a ! EventList("e", "f")
    a ! EventList("g", "h")
    a ! EventList("i", "j")
    Thread.sleep(2000)
    c ! EventList("a")
    c ! EventList("b")
    c ! EventList("c")
    expectMsg(EventList("e", "a"))
    expectMsg(EventList("e", "b"))
    expectMsg(EventList("e", "a"))
    expectMsg(EventList("e", "b"))
    stopActors(a, b, c, graph)
  }

  test("Nested - CEP operators") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val query: HListQuery[Either[Int, Float]::Either[Float, Boolean]::HNil] =
      stream[Int::HNil]("A")
      .and(stream[Float::HNil]("B"))
      .or(sequence(nStream[Float::HNil]("B") -> nStream[Boolean::HNil]("C")))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    a ! EventList(21)
    a ! EventList(42)
    Thread.sleep(2000)
    b ! EventList(21.0f)
    b ! EventList(42.0f)
    Thread.sleep(2000)
    c ! EventList(true)
    expectMsg(EventList(Left(21), Left(21.0f)))
    expectMsg(EventList(Right(21.0f), Right(true)))
    stopActors(a, b, graph)
  }
}
