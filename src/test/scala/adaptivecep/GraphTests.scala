package adaptivecep

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.factory._
import adaptivecep.publishers._
import adaptivecep.graph.qos._
import shapeless.{HList, ::, HNil}

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
    a ! Event1("42")
    expectMsg(Event1("42"))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event2(42, 42)
    expectMsg(Event2(42, 42))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Long::Long::Long::HNil] = stream[Long::Long::Long::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event3(42l, 42l, 42l)
    expectMsg(Event3(42l, 42l, 42l))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Float::Float::Float::Float::HNil] = stream[Float::Float::Float::Float::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event4(42f, 42f, 42f, 42f)
    expectMsg(Event4(42f, 42f, 42f, 42f))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Double::Double::Double::Double::Double::HNil] =
      stream[Double::Double::Double::Double::Double::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event5(42.0, 42.0, 42.0, 42.0, 42.0)
    expectMsg(Event5(42.0, 42.0, 42.0, 42.0, 42.0))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Boolean::Boolean::Boolean::Boolean::Boolean::Boolean::HNil] =
      stream[Boolean::Boolean::Boolean::Boolean::Boolean::Boolean::HNil]("A")
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event6(true, true, true, true, true, true)
    expectMsg(Event6(true, true, true, true, true, true))
    stopActors(a, graph)
  }


  test("LeafNode - SequenceNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event2(21, 42)
    Thread.sleep(2000)
    b ! Event2("21", "42")
    expectMsg(Event4(21, 42, "21", "42"))
    stopActors(a, b, graph)
  }

  test("LeafNode - SequenceNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: HListQuery[Int::Int::String::String::HNil] =
      sequence(nStream[Int::Int::HNil]("A") -> nStream[String::String::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event2(1, 1)
    Thread.sleep(2000)
    a ! Event2(2, 2)
    Thread.sleep(2000)
    a ! Event2(3, 3)
    Thread.sleep(2000)
    b ! Event2("1", "1")
    Thread.sleep(2000)
    b ! Event2("2", "2")
    Thread.sleep(2000)
    b ! Event2("3", "3")
    expectMsg(Event4(1, 1, "1", "1"))
    stopActors(a, b, graph)
  }

  test("UnaryNode - FilterNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("A")
      .where(x => x.head >= x.last)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event2(41, 42)
    a ! Event2(42, 42)
    a ! Event2(43, 42)
    expectMsg(Event2(42, 42))
    expectMsg(Event2(43, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::Int::HNil] =
      stream[Int::Int::HNil]("A")
      .where(x => x.head <= x.last)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event2(41, 42)
    a ! Event2(42, 42)
    a ! Event2(43, 42)
    expectMsg(Event2(41, 42))
    expectMsg(Event2(42, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Long::HNil] =
      stream[Long::HNil]("A")
      .where(_.head == 42l)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event1(41l)
    a ! Event1(42l)
    expectMsg(Event1(42l))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Float::HNil] =
      stream[Float::HNil]("A")
      .where(_.head > 41f)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event1(41f)
    a ! Event1(42f)
    expectMsg(Event1(42f))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 5") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Double::HNil] =
      stream[Double::HNil]("A")
      .where(_.head < 42.0)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event1(41.0)
    a ! Event1(42.0)
    expectMsg(Event1(41.0))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 6") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query =
      stream[Boolean::HNil]("A")
      .where(_.head != true)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event1(true)
    a ! Event1(false)
    expectMsg(Event1(false))
    stopActors(a, graph)
  }

  // TODO dropelement implementation needed
  /*test("UnaryNode - DropElemNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query: HListQuery[Int::HNil] =
      stream[Int::Int::HNil]("A")
      .dropElem2()
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event2(21, 42)
    a ! Event2(42, 21)
    expectMsg(Event1(21))
    expectMsg(Event1(42))
    stopActors(a, graph)
  }

  test("UnaryNode - DropElemNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query2[String, String] =
      stream[String, String, String, String]("A")
      .dropElem1()
      .dropElem2()
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event4("a", "b", "c", "d")
    a ! Event4("e", "f", "g", "h")
    expectMsg(Event2("b", "d"))
    expectMsg(Event2("f", "h"))
    stopActors(a, graph)
  }
  */

  test("UnaryNode - SelfJoinNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val query = //: HListQuery[String::String::String::String::HNil] =
      stream[String::String::HNil]("A")
      .selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event2("a", "b")
    a ! Event2("c", "d")
    a ! Event2("e", "f")
    expectMsg(Event4("a", "b", "a", "b"))
    expectMsg(Event4("a", "b", "c", "d"))
    expectMsg(Event4("c", "d", "a", "b"))
    expectMsg(Event4("c", "d", "c", "d"))
    expectMsg(Event4("e", "f", "a", "b"))
    expectMsg(Event4("e", "f", "c", "d"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val query = //: HListQuery[String::String::String::String::HNil] =
      stream[String::String::HNil]("A")
      .selfJoin(slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event2("a", "b")
    a ! Event2("c", "d")
    a ! Event2("e", "f")
    expectMsg(Event4("a", "b", "a", "b"))
    expectMsg(Event4("c", "d", "a", "b"))
    expectMsg(Event4("c", "d", "c", "d"))
    expectMsg(Event4("a", "b", "c", "d"))
    expectMsg(Event4("e", "f", "c", "d"))
    expectMsg(Event4("e", "f", "e", "f"))
    expectMsg(Event4("a", "b", "e", "f"))
    expectMsg(Event4("c", "d", "e", "f"))
    stopActors(a, graph)
  }

  test("BinaryNode - JoinNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query = // : HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event3("a", true, "b")
    a ! Event3("c", true, "d")
    a ! Event3("e", true, "f")
    a ! Event3("g", true, "h")
    a ! Event3("i", true, "j")
    Thread.sleep(2000)
    b ! Event2(1, 2)
    b ! Event2(3, 4)
    b ! Event2(5, 6)
    b ! Event2(7, 8)
    expectMsg(Event5("a", true, "b", 1, 2))
    expectMsg(Event5("c", true, "d", 1, 2))
    expectMsg(Event5("e", true, "f", 1, 2))
    expectMsg(Event5("a", true, "b", 3, 4))
    expectMsg(Event5("c", true, "d", 3, 4))
    expectMsg(Event5("e", true, "f", 3, 4))
    expectMsg(Event5("a", true, "b", 5, 6))
    expectMsg(Event5("c", true, "d", 5, 6))
    expectMsg(Event5("e", true, "f", 5, 6))
    expectMsg(Event5("a", true, "b", 7, 8))
    expectMsg(Event5("c", true, "d", 7, 8))
    expectMsg(Event5("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query = // : HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    b ! Event2(1, 2)
    b ! Event2(3, 4)
    b ! Event2(5, 6)
    b ! Event2(7, 8)
    Thread.sleep(2000)
    a ! Event3("a", true, "b")
    a ! Event3("c", true, "d")
    a ! Event3("e", true, "f")
    a ! Event3("g", true, "h")
    a ! Event3("i", true, "j")
    expectMsg(Event5("a", true, "b", 5, 6))
    expectMsg(Event5("a", true, "b", 7, 8))
    expectMsg(Event5("c", true, "d", 5, 6))
    expectMsg(Event5("c", true, "d", 7, 8))
    expectMsg(Event5("e", true, "f", 5, 6))
    expectMsg(Event5("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test ("BinaryNode - JoinNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query = // : HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event3("a", true, "b")
    a ! Event3("c", true, "d")
    a ! Event3("e", true, "f")
    a ! Event3("g", true, "h")
    a ! Event3("i", true, "j")
    Thread.sleep(2000)
    b ! Event2(1, 2)
    b ! Event2(3, 4)
    b ! Event2(5, 6)
    b ! Event2(7, 8)
    expectMsg(Event5("e", true, "f", 1, 2))
    expectMsg(Event5("g", true, "h", 1, 2))
    expectMsg(Event5("i", true, "j", 1, 2))
    expectMsg(Event5("e", true, "f", 3, 4))
    expectMsg(Event5("g", true, "h", 3, 4))
    expectMsg(Event5("i", true, "j", 3, 4))
    expectMsg(Event5("e", true, "f", 5, 6))
    expectMsg(Event5("g", true, "h", 5, 6))
    expectMsg(Event5("i", true, "j", 5, 6))
    expectMsg(Event5("e", true, "f", 7, 8))
    expectMsg(Event5("g", true, "h", 7, 8))
    expectMsg(Event5("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 4") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: HListQuery[Int::Int::HNil] = stream[Int::Int::HNil]("B")
    val query = // : HListQuery[String::Boolean::String::Int::Int::HNil] =
      stream[String::Boolean::String::HNil]("A")
      .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    b ! Event2(1, 2)
    b ! Event2(3, 4)
    b ! Event2(5, 6)
    b ! Event2(7, 8)
    Thread.sleep(2000)
    a ! Event3("a", true, "b")
    a ! Event3("c", true, "d")
    a ! Event3("e", true, "f")
    a ! Event3("g", true, "h")
    a ! Event3("i", true, "j")
    expectMsg(Event5("a", true, "b", 5, 6))
    expectMsg(Event5("a", true, "b", 7, 8))
    expectMsg(Event5("c", true, "d", 5, 6))
    expectMsg(Event5("c", true, "d", 7, 8))
    expectMsg(Event5("e", true, "f", 5, 6))
    expectMsg(Event5("e", true, "f", 7, 8))
    expectMsg(Event5("g", true, "h", 5, 6))
    expectMsg(Event5("g", true, "h", 7, 8))
    expectMsg(Event5("i", true, "j", 5, 6))
    expectMsg(Event5("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("Binary Node - ConjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query = // : HListQuery[Int::Float::HNil] =
      stream[Int::HNil]("A")
      .and(stream[Float::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event1(21)
    b ! Event1(21.0f)
    Thread.sleep(2000)
    a ! Event1(42)
    b ! Event1(42.0f)
    expectMsg(Event2(21, 21.0f))
    expectMsg(Event2(42, 42.0f))
    stopActors(a, b, graph)
  }

  test("Binary Node - ConjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query = // : HListQuery[Int::Float::HNil] =
      stream[Int::HNil]("A")
      .and(stream[Float::HNil]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event1(21)
    a ! Event1(42)
    Thread.sleep(2000)
    b ! Event1(21.0f)
    b ! Event1(42.0f)
    expectMsg(Event2(21, 21.0f))
    stopActors(a, b, graph)
  }

  // TODO need disjunction and dropElem implementation
  /*test("Binary Node - DisjunctionNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val query: Query2[Either[Int, String], Either[Int, String]] =
      stream[Int, Int]("A")
      .or(stream[String, String]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Created)
    a ! Event2(21, 42)
    Thread.sleep(2000)
    b ! Event2("21", "42")
    expectMsg(Event2(Left(21), Left(42)))
    expectMsg(Event2(Right("21"), Right("42")))
    stopActors(a, b, graph)
  }

  test("Binary Node - DisjunctionNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val query:
      Query3[Either[Either[Int, String], Boolean],
             Either[Either[Int, String], Boolean],
             Either[Unit,                Boolean]] =
      stream[Int, Int]("A")
        .or(stream[String, String]("B"))
        .or(stream[Boolean, Boolean, Boolean]("C"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    a ! Event2(21, 42)
    Thread.sleep(2000)
    b ! Event2("21", "42")
    Thread.sleep(2000)
    c ! Event3(true, false, true)
    expectMsg(Event3(Left(Left(21)), Left(Left(42)), Left(())))
    expectMsg(Event3(Left(Right("21")), Left(Right("42")), Left(())))
    expectMsg(Event3(Right(true), Right(false), Right(true)))
    stopActors(a, b, c, graph)
  }

  test("Nested - SP operators") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val sq1: Query2[String, String] = stream[String, String]("A")
    val sq2: Query2[Int, Int] = stream[Int, Int]("B")
    val sq3: Query1[String] = stream[String]("C")
    val sq4: Query4[String, String, Int, Int] =
      sq1.join(sq2, tumblingWindow(3.instances), tumblingWindow(2.instances))
    val sq5: Query2[String, String] =
      sq3.selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
    val sq6: Query6[String, String, Int, Int, String, String] =
      sq4.join(sq5, tumblingWindow(1.instances), tumblingWindow(4.instances))
    val sq7: Query6[String, String, Int, Int, String, String] =
      sq6.where((_, _, e3, e4, _, _) => e3 < e4)
    val query: Query2[String, String] =
      sq7
      .dropElem2()
      .dropElem2()
      .dropElem2()
      .dropElem2()
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    b ! Event2(1, 2)
    b ! Event2(3, 4)
    b ! Event2(5, 6)
    b ! Event2(7, 8)
    Thread.sleep(2000)
    a ! Event2("a", "b")
    a ! Event2("c", "d")
    a ! Event2("e", "f")
    a ! Event2("g", "h")
    a ! Event2("i", "j")
    Thread.sleep(2000)
    c ! Event1("a")
    c ! Event1("b")
    c ! Event1("c")
    expectMsg(Event2("e", "a"))
    expectMsg(Event2("e", "b"))
    expectMsg(Event2("e", "a"))
    expectMsg(Event2("e", "b"))
    stopActors(a, b, c, graph)
  }

  test("Nested - CEP operators") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val c: ActorRef = createTestPublisher("C")
    val query: Query2[Either[Int, Float], Either[Float, Boolean]] =
      stream[Int]("A")
      .and(stream[Float]("B"))
      .or(sequence(nStream[Float]("B") -> nStream[Boolean]("C")))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Created)
    a ! Event1(21)
    a ! Event1(42)
    Thread.sleep(2000)
    b ! Event1(21.0f)
    b ! Event1(42.0f)
    Thread.sleep(2000)
    c ! Event1(true)
    expectMsg(Event2(Left(21), Left(21.0f)))
    expectMsg(Event2(Right(21.0f), Right(true)))
    stopActors(a, b, graph)
  }
  */
}
