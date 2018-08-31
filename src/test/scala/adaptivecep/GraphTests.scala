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
import shapeless.labelled.KeyTag
import shapeless.record.Record
import shapeless.{::, HNil, Nat, Witness}
import shapeless.syntax.singleton._

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
    a ! Event("42")
    expectMsg(Event("42"))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] = tstream[(Int, Int)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(42, 42)
    expectMsg(Event(42, 42))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Long::Long::Long::HNil] = tstream[(Long, Long, Long)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(42l, 42l, 42l)
    expectMsg(Event(42l, 42l, 42l))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Float::Float::Float::Float::HNil] = tstream[(Float, Float, Float, Float)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(42f, 42f, 42f, 42f)
    expectMsg(Event(42f, 42f, 42f, 42f))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Double::Double::Double::Double::Double::HNil] =
      tstream[(Double, Double, Double, Double, Double)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(42.0, 42.0, 42.0, 42.0, 42.0)
    expectMsg(Event(42.0, 42.0, 42.0, 42.0, 42.0))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: TupleQuery[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)] =
      tstream[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(true, true, true, true, true, true)
    expectMsg(Event(true, true, true, true, true, true))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 7") {
    val a: ActorRef = createTestPublisher("A")
    val query: TupleQuery[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Int)] =
      tstream[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Int)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(true, true, true, true, true, true, 12)
    expectMsg(Event(true, true, true, true, true, true, 12))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 8") {
    val a: ActorRef = createTestPublisher("A")
    val query: TupleQuery[(Boolean, String, Boolean, Boolean, Boolean, Boolean, Boolean, Int)] =
      tstream[(Boolean, String, Boolean, Boolean, Boolean, Boolean, Boolean, Int)]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(true, "test", true, true, true, true, true, 12)
    expectMsg(Event(true, "test", true, true, true, true, true, 12))
    stopActors(a, graph)
  }

  test("LeafNode - SequenceNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event(21, 42)
    Thread.sleep(2000)
    b ! Event("21", "42")
    expectMsg(Event(21, 42, "21", "42"))
    stopActors(a, b, graph)
  }

  test("LeafNode - SequenceNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[String::String::HNil]("B"))
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

  test("UnaryNode - FilterNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("A")
      .where(x => x.head >= x.last)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(41, 42)
    a ! Event(42, 42)
    a ! Event(43, 42)
    expectMsg(Event(42, 42))
    expectMsg(Event(43, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("A")
      .where(x => x.head <= x.last)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(41, 42)
    a ! Event(42, 42)
    a ! Event(43, 42)
    expectMsg(Event(41, 42))
    expectMsg(Event(42, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Long::HNil] =
      stream[Long::HNil]("A")
      .where(_.head == 42l)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(41l)
    a ! Event(42l)
    expectMsg(Event(42l))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Float::HNil] =
      stream[Float::HNil]("A")
      .where(_.head > 41f)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(41f)
    a ! Event(42f)
    expectMsg(Event(42f))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Double::HNil] =
      stream[Double::HNil]("A")
      .where(_.head < 42.0)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(41.0)
    a ! Event(42.0)
    expectMsg(Event(41.0))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query =
      stream[Boolean::HNil]("A")
      .where(_.head != true)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(true)
    a ! Event(false)
    expectMsg(Event(false))
    stopActors(a, graph)
  }

  test("UnaryNode - DropElemNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::HNil] =
      stream[Int::Int::HNil]("A")
        .drop(Nat._2)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event(21, 42)
    a ! Event(42, 21)
    expectMsg(Event(21))
    expectMsg(Event(42))
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
    a ! Event("a", "b", "c", "d")
    a ! Event("e", "f", "g", "h")
    expectMsg(Event("b", "d"))
    expectMsg(Event("f", "h"))
    stopActors(a, graph)
  }

  test("UnaryNode - DropElemNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::String::String::String::String::String::HNil] =
      stream[String::String::String::String::String::String::String::String::HNil]("A")
        .drop(Nat._8)
        .drop(Nat._1)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("a", "b", "c", "d", "e", "f", "g", "h")
    a ! Event("i", "j", "k", "l", "m", "n", "o", "p")
    expectMsg(Event("b", "c", "d", "e", "f", "g"))
    expectMsg(Event("j", "k", "l", "m", "n", "o"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::String::String::String::HNil] =
      stream[String::String::HNil]("A")
      .selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("a", "b")
    a ! Event("c", "d")
    a ! Event("e", "f")
    expectMsg(Event("a", "b", "a", "b"))
    expectMsg(Event("a", "b", "c", "d"))
    expectMsg(Event("c", "d", "a", "b"))
    expectMsg(Event("c", "d", "c", "d"))
    expectMsg(Event("e", "f", "a", "b"))
    expectMsg(Event("e", "f", "c", "d"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[String::String::String::String::HNil] =
      stream[String::String::HNil]("A")
      .selfJoin(slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("a", "b")
    a ! Event("c", "d")
    a ! Event("e", "f")
    expectMsg(Event("a", "b", "a", "b"))
    expectMsg(Event("c", "d", "a", "b"))
    expectMsg(Event("c", "d", "c", "d"))
    expectMsg(Event("a", "b", "c", "d"))
    expectMsg(Event("e", "f", "c", "d"))
    expectMsg(Event("e", "f", "e", "f"))
    expectMsg(Event("a", "b", "e", "f"))
    expectMsg(Event("c", "d", "e", "f"))
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

  test("BinaryNode - JoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
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

  test ("BinaryNode - JoinNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
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
    expectMsg(Event("e", true, "f", 1, 2))
    expectMsg(Event("g", true, "h", 1, 2))
    expectMsg(Event("i", true, "j", 1, 2))
    expectMsg(Event("e", true, "f", 3, 4))
    expectMsg(Event("g", true, "h", 3, 4))
    expectMsg(Event("i", true, "j", 3, 4))
    expectMsg(Event("e", true, "f", 5, 6))
    expectMsg(Event("g", true, "h", 5, 6))
    expectMsg(Event("i", true, "j", 5, 6))
    expectMsg(Event("e", true, "f", 7, 8))
    expectMsg(Event("g", true, "h", 7, 8))
    expectMsg(Event("i", true, "j", 7, 8))
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
    expectMsg(Event("g", true, "h", 5, 6))
    expectMsg(Event("g", true, "h", 7, 8))
    expectMsg(Event("i", true, "j", 5, 6))
    expectMsg(Event("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinOnNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query: HListQuery[String::Boolean::Int::Int::HNil] =
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

  test("Binary Node - ConjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Float::HNil] =
      stream[Int::HNil]("A")
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

  test("Binary Node - ConjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Float::HNil] =
      stream[Int::HNil]("A")
      .and(stream[Float::HNil]("B"))
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

  test("Binary Node - DisjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Either[Int, String]::Either[Int, String]::HNil] =
      stream[Int::Int::HNil]("A")
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
      sq6.where(x => x(Nat._2) < x(Nat._3))
    val query: HListQuery[String::String::HNil] =
      sq7
        .drop(Nat._2)
        .drop(Nat._2)
        .drop(Nat._2)
        .drop(Nat._2)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    b ! Event(1, 2)
    b ! Event(3, 4)
    b ! Event(5, 6)
    b ! Event(7, 8)
    Thread.sleep(2000)
    a ! Event("a", "b")
    a ! Event("c", "d")
    a ! Event("e", "f")
    a ! Event("g", "h")
    a ! Event("i", "j")
    Thread.sleep(2000)
    c ! Event("a")
    c ! Event("b")
    c ! Event("c")
    expectMsg(Event("e", "a"))
    expectMsg(Event("e", "b"))
    expectMsg(Event("e", "a"))
    expectMsg(Event("e", "b"))
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
    a ! Event(21)
    a ! Event(42)
    Thread.sleep(2000)
    b ! Event(21.0f)
    b ! Event(42.0f)
    Thread.sleep(2000)
    c ! Event(true)
    expectMsg(Event(Left(21), Left(21.0f)))
    expectMsg(Event(Right(21.0f), Right(true)))
    stopActors(a, b, graph)
  }

  // Tests that cover the usage of extensible records
  val wName = Witness("name")
  val wOther = Witness("other")

  test("Record - JoinNode") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Record.`"name" -> Int`.T] = stream[Record.`"name" -> Int`.T]("B")
    val query: HListQuery[Record.`"other" -> Boolean, "name" -> Int`.T] =
      stream[Record.`"other" -> Boolean`.T]("A")
        .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("other" ->> true)
    a ! Event("other" ->> false)
    a ! Event("other" ->> true)
    a ! Event("other" ->> false)
    a ! Event("other" ->> true)
    Thread.sleep(2000)
    b ! Event("name" ->> 1)
    b ! Event("name" ->> 2)
    b ! Event("name" ->> 3)
    b ! Event("name" ->> 4)
    expectMsg(Event("other" ->> true,  1))
    expectMsg(Event("other" ->> false, 1))
    expectMsg(Event("other" ->> true,  1))
    expectMsg(Event("other" ->> true,  2))
    expectMsg(Event("other" ->> false, 2))
    expectMsg(Event("other" ->> true,  2))
    expectMsg(Event("other" ->> true,  3))
    expectMsg(Event("other" ->> false, 3))
    expectMsg(Event("other" ->> true,  3))
    expectMsg(Event("other" ->> true,  4))
    expectMsg(Event("other" ->> false, 4))
    expectMsg(Event("other" ->> true,  4))
    stopActors(a, b, graph)
  }

  test("Record - JoinOnNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Record.`"name" -> Int`.T] = stream[Record.`"name" -> Int`.T]("B")
    val query: HListQuery[Record.`"other" -> Boolean, "age" -> Int`.T] =
      stream[Record.`"other" -> Boolean, "age" -> Int`.T]("A")
        .joinOn(sq, wAge, wName, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("other" ->> true,  "age" ->> 1)
    a ! Event("other" ->> false, "age" ->> 2)
    a ! Event("other" ->> true,  "age" ->> 3)
    a ! Event("other" ->> false, "age" ->> 4)
    a ! Event("other" ->> true,  "age" ->> 5)
    Thread.sleep(2000)
    b ! Event("name" ->> 1)
    b ! Event("name" ->> 2)
    b ! Event("name" ->> 3)
    b ! Event("name" ->> 4)
    expectMsg(Event("other" ->> true,  "age" ->> 1))
    expectMsg(Event("other" ->> false, "age" ->> 2))
    expectMsg(Event("other" ->> true,  "age" ->> 3))
    stopActors(a, b, graph)
  }

  test("Record - SelfJoinNode") {
    val a: ActorRef = createTestPublisher("A")
    val query =
      stream[Record.`"name" -> String, "other" -> String`.T]("A")
        .selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> "a", "other" ->> "b")
    a ! Event("name" ->> "c", "other" ->> "d")
    a ! Event("name" ->> "e", "other" ->> "f")
    expectMsg(Event("name" ->> "a", "other" ->> "b", "name"  ->> "a", "other" ->> "b"))
    expectMsg(Event("name" ->> "a", "other" ->> "b", "name"  ->> "c", "other" ->> "d"))
    expectMsg(Event("name" ->> "c", "other" ->> "d", "name"  ->> "a", "other" ->> "b"))
    expectMsg(Event("name" ->> "c", "other" ->> "d", "name"  ->> "c", "other" ->> "d"))
    expectMsg(Event("name" ->> "e", "other" ->> "f", "name"  ->> "a", "other" ->> "b"))
    expectMsg(Event("name" ->> "e", "other" ->> "f", "name"  ->> "c", "other" ->> "d"))
    stopActors(a, graph)
  }

  test("Record - FilterNode") {
    import shapeless.record._
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Record.`"name" -> Boolean`.T] =
      stream[Record.`"name" -> Boolean`.T]("A")
        .where(x => x("name") == true)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true)
    a ! Event("name" ->> false)
    a ! Event("name" ->> true)
    expectMsg(Event("name" ->> true))
    expectMsg(Event("name" ->> true))
    stopActors(a, graph)
  }

  test("Record - DropElemNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int with KeyTag[wOther.T, Int]::HNil] =
      stream[Boolean with KeyTag[wName.T, Boolean]::Int with KeyTag[wOther.T, Int]::HNil]("A")
        .drop(wName)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3)
    expectMsg(Event("other" ->> 3))
    stopActors(a, graph)
  }

  test("Record - DropElemNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Boolean with KeyTag[wName.T, Boolean]::HNil] =
      stream[Boolean with KeyTag[wName.T, Boolean]::Int with KeyTag[wOther.T, Int]::HNil]("A")
        .drop(wOther)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3)
    expectMsg(Event("name" ->> true))
    stopActors(a, graph)
  }

  val wAge = Witness("age")
  test("Record - DropElemNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Record.`"name" -> Boolean, "other" -> Int`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "age" -> Int`.T]("A")
        .drop(wAge)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3, "age" ->> 27)
    expectMsg(Event("name" ->> true, "other" ->> 3))
    stopActors(a, graph)
  }

  test("Record - DropElemNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Record.`"name" -> Boolean, "age" -> Int`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "age" -> Int`.T]("A")
        .drop(wOther)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3, "age" ->> 27)
    expectMsg(Event("name" ->> true, "age" ->> 27))
    stopActors(a, graph)
  }

  test("Record - DropElemNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Record.`"name" -> Boolean, "other" -> String`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "other" -> String`.T]("A")
        .drop(wOther)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3, "other" ->> "Test")
    expectMsg(Event("name" ->> true, "other" ->> "Test"))
    stopActors(a, graph)
  }

  test("Record - DropElemNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Record.`"name" -> Boolean`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "other" -> String`.T]("A")
        .drop(wOther)
        .drop(wOther)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3, "other" ->> "Test")
    expectMsg(Event("name" ->> true))
    stopActors(a, graph)
  }

  test("Mixed - DropElemNode") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Record.`"other" -> String`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "other" -> String`.T]("A")
        .drop(wOther)
        .drop(Nat._1)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3, "other" ->> "Test")
    expectMsg(Event("other" ->> "Test"))
    stopActors(a, graph)
  }

  test("Record - ConjunctionNode") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query = // : HListQuery[Boolean with KeyTag[wName.T,  Boolean] :: HNil] =
      stream[Record.`"name" -> Boolean`.T]("A")
        .and(stream[Int::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true)
    b ! Event(1)
    expectMsg(Event("name" ->> true, 1))
    a ! Event("name" ->> false)
    b ! Event(4)
    expectMsg(Event("name" ->> false, 4))
    stopActors(a, b, graph)
  }

  test("Record - DisjunctionNode") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Either[Boolean with KeyTag[wName.T, Boolean], Int] :: HNil] =
      stream[Record.`"name" -> Boolean`.T]("A")
      .or(stream[Int::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true)
    expectMsg(Event(Left("name" ->> true)))
    b ! Event(1)
    expectMsg(Event(Right(1)))
    stopActors(a, b, graph)
  }

  test("Record - Mixed - 1") {
    import shapeless.record._
    val a: ActorRef = createTestPublisher("A")
    val query = //: HListQuery[Record.`"name" -> Boolean, "other" -> String`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "other" -> String`.T]("A")
        .where(x => x("name") == false)
        .drop(wOther)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 1, "last" ->> "no")
    a ! Event("name" ->> false, "other" ->> 2, "last" ->> "yes")
    a ! Event("name" ->> false, "other" ->> 3, "last" ->> "maybe")
    expectMsg(Event("name" ->> false, "last" ->> "yes"))
    expectMsg(Event("name" ->> false, "last" ->> "maybe"))
    stopActors(a, graph)
  }

  test("Record - Mixed - 2") {
    import shapeless.record._
    val a: ActorRef = createTestPublisher("A")
    val query = //: HListQuery[Record.`"name" -> Boolean, "other" -> String`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "last" -> String`.T]("A")
        .drop(Witness("last"))
        .where(x => x("other") != 2)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 1, "last" ->> "no")
    a ! Event("name" ->> false, "other" ->> 2, "last" ->> "yes")
    a ! Event("name" ->> false, "other" ->> 3, "last" ->> "maybe")
    expectMsg(Event("name" ->> true, "other" ->> 1))
    expectMsg(Event("name" ->> false, "other" ->> 3))
    stopActors(a, graph)
  }

  test("Record - Mixed - 3") {
    import shapeless.record._
    val a: ActorRef = createTestPublisher("A")
    val query = //: HListQuery[Record.`"name" -> Boolean, "other" -> String`.T] =
      stream[Record.`"name" -> Boolean, "other" -> Int, "other" -> String`.T]("A")
        .where(x => x("other") != 2)
        .where(x => x.head == false)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 1, "last" ->> "no")
    a ! Event("name" ->> false, "other" ->> 2, "last" ->> "yes")
    a ! Event("name" ->> false, "other" ->> 3, "last" ->> "maybe")
    expectMsg(Event("name" ->> false, "other" ->> 3, "last" ->> "maybe"))
    stopActors(a, graph)
  }

  // Tests that cover the corner cases when we use HListQuery[HNil]
  test("Empty HListQuery - Simple") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[HNil] =
      stream[HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event()
    a ! Event()
    expectMsg(Event())
    expectMsg(Event())
    stopActors(a, graph)
  }

  test("Empty HListQuery - DropNode") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[HNil] = stream[Int::HNil]("A").drop(Nat._1)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event()
    expectMsg(Event())
    stopActors(a, graph)
  }

  test("Empty HListQuery - SelfJoinNode") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[HNil] =
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

  test("Empty HListQuery - Join - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[HNil] = stream[HNil]("A")
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

  test("Empty HListQuery - Join - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::HNil] = stream[Int::HNil]("A")
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

  test("Empty HListQuery - Join - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::HNil] = stream[HNil]("A")
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

  test("Empty HListQuery - ConjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[HNil] =
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

  test("Empty HListQuery - ConjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::HNil] =
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

  test("Empty HListQuery - ConjunctionNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Boolean::HNil] =
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

  test("Empty HListQuery - DisjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[HNil] =
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

  test("Empty HListQuery - DisjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Either[Int, Unit]::HNil] =
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

  test("Empty HListQuery - DisjunctionNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Either[Unit, Int]::HNil] =
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
