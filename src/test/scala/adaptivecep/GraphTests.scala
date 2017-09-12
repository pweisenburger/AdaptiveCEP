package adaptivecep

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class GraphTests extends TestKit(ActorSystem()) with FunSuiteLike with BeforeAndAfterAll {

  def getTestPublisher(name: String): ActorRef =
    system.actorOf(Props(TestPublisher()), name)

  def getTestGraph(query: Query, publishers: Map[String, ActorRef], testActor: ActorRef): ActorRef = query match {
    case streamQuery: StreamQuery =>
      system.actorOf(Props(StreamNode(streamQuery, publishers, DummyMonitorFactory(), DummyMonitorFactory(), Some(testActor ! _))), "stream")
    case filterQuery: FilterQuery =>
      system.actorOf(Props(FilterNode(filterQuery, publishers, DummyMonitorFactory(), DummyMonitorFactory(), Some(testActor ! _))), "filter")
    case selectQuery: SelectQuery =>
      system.actorOf(Props(SelectNode(selectQuery, publishers, DummyMonitorFactory(), DummyMonitorFactory(), Some(testActor ! _))), "select")
    case selfJoinQuery: SelfJoinQuery =>
      system.actorOf(Props(SelfJoinNode(selfJoinQuery, publishers, DummyMonitorFactory(), DummyMonitorFactory(), Some(testActor ! _))), "selfjoin")
    case joinQuery: JoinQuery =>
      system.actorOf(Props(JoinNode(joinQuery, publishers, DummyMonitorFactory(), DummyMonitorFactory(), Some(testActor ! _))), "join")
  }

  // Source: http://doc.akka.io/docs/akka/current/scala/testing.html#Watching_Other_Actors_from_Probes
  def stopActor(actor: ActorRef): Unit = {
    val probe = TestProbe()
    probe watch actor
    actor ! PoisonPill
    probe.expectTerminated(actor)
  }

  def stopActors(actors: ActorRef*): Unit =
    actors.foreach(stopActor)

  override def afterAll(): Unit =
    system.terminate()

  test("LeafNode - StreamNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query1[String] = stream[String]("A")
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event1("42")
    expectMsg(Right(Event1("42")))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query2[Int, Int] = stream[Int, Int]("A")
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event2(42, 42)
    expectMsg(Right(Event2(42, 42)))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 3") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query3[Long, Long, Long] = stream[Long, Long, Long]("A")
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event3(42l, 42l, 42l)
    expectMsg(Right(Event3(42l, 42l, 42l)))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 4") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query4[Float, Float, Float, Float] = stream[Float, Float, Float, Float]("A")
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event4(42f, 42f, 42f, 42f)
    expectMsg(Right(Event4(42f, 42f, 42f, 42f)))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 5") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query5[Double, Double, Double, Double, Double] = stream[Double, Double, Double, Double, Double]("A")
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event5(42.0, 42.0, 42.0, 42.0, 42.0)
    expectMsg(Right(Event5(42.0, 42.0, 42.0, 42.0, 42.0)))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 6") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query6[Boolean, Boolean, Boolean, Boolean, Boolean, Boolean] = stream[Boolean, Boolean, Boolean, Boolean, Boolean, Boolean]("A")
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event6(true, true, true, true, true, true)
    expectMsg(Right(Event6(true, true, true, true, true, true)))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query2[Int, Int] =
      stream[Int, Int]("A")
      .where(_ >= _)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event2(41, 42)
    a ! Event2(42, 42)
    a ! Event2(43, 42)
    expectMsg(Right(Event2(42, 42)))
    expectMsg(Right(Event2(43, 42)))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query2[Int, Int] =
      stream[Int, Int]("A")
      .where(_ <= _)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event2(41, 42)
    a ! Event2(42, 42)
    a ! Event2(43, 42)
    expectMsg(Right(Event2(41, 42)))
    expectMsg(Right(Event2(42, 42)))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 3") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query1[Long] =
      stream[Long]("A")
      .where(_ == 42l)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event1(41l)
    a ! Event1(42l)
    expectMsg(Right(Event1(42l)))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 4") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query1[Float] =
      stream[Float]("A")
      .where(_ > 41f)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event1(41f)
    a ! Event1(42f)
    expectMsg(Right(Event1(42f)))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 5") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query1[Double] =
      stream[Double]("A")
      .where(_ < 42.0)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event1(41.0)
    a ! Event1(42.0)
    expectMsg(Right(Event1(41.0)))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 6") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query =
      stream[Boolean]("A")
      .where(_ != true)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event1(true)
    a ! Event1(false)
    expectMsg(Right(Event1(false)))
    stopActors(a, graph)
  }

  test("UnaryNode - SelectNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query1[Int] =
      stream[Int, Int]("A")
      .removeElement2()
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event2(21, 42)
    a ! Event2(42, 21)
    expectMsg(Right(Event1(21)))
    expectMsg(Right(Event1(42)))
    stopActors(a, graph)
  }

  test("UnaryNode - SelectNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query2[String, String] =
      stream[String, String, String, String]("A")
      .removeElement1()
      .removeElement2()
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event4("a", "b", "c", "d")
    a ! Event4("e", "f", "g", "h")
    expectMsg(Right(Event2("b", "d")))
    expectMsg(Right(Event2("f", "h")))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query4[String, String, String, String] =
      stream[String, String]("A")
      .selfJoin(tumblingWindow(3 instances), tumblingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event2("a", "b")
    a ! Event2("c", "d")
    a ! Event2("e", "f")
    expectMsg(Right(Event4("a", "b", "a", "b")))
    expectMsg(Right(Event4("a", "b", "c", "d")))
    expectMsg(Right(Event4("c", "d", "a", "b")))
    expectMsg(Right(Event4("c", "d", "c", "d")))
    expectMsg(Right(Event4("e", "f", "a", "b")))
    expectMsg(Right(Event4("e", "f", "c", "d")))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query4[String, String, String, String] =
      stream[String, String]("A")
      .selfJoin(slidingWindow(3 instances), slidingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Left(GraphCreated))
    a ! Event2("a", "b")
    a ! Event2("c", "d")
    a ! Event2("e", "f")
    expectMsg(Right(Event4("a", "b", "a", "b")))
    expectMsg(Right(Event4("c", "d", "a", "b")))
    expectMsg(Right(Event4("c", "d", "c", "d")))
    expectMsg(Right(Event4("a", "b", "c", "d")))
    expectMsg(Right(Event4("e", "f", "c", "d")))
    expectMsg(Right(Event4("e", "f", "e", "f")))
    expectMsg(Right(Event4("a", "b", "e", "f")))
    expectMsg(Right(Event4("c", "d", "e", "f")))
    stopActors(a, graph)
  }

  test("BinaryNode - JoinNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq: Query2[Int, Int] = stream[Int, Int]("B")
    val query: Query5[String, Boolean, String, Int, Int] =
      stream[String, Boolean, String]("A")
      .join(sq, tumblingWindow(3 instances), tumblingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Left(GraphCreated))
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
    expectMsg(Right(Event5("a", true, "b", 1, 2)))
    expectMsg(Right(Event5("c", true, "d", 1, 2)))
    expectMsg(Right(Event5("e", true, "f", 1, 2)))
    expectMsg(Right(Event5("a", true, "b", 3, 4)))
    expectMsg(Right(Event5("c", true, "d", 3, 4)))
    expectMsg(Right(Event5("e", true, "f", 3, 4)))
    expectMsg(Right(Event5("a", true, "b", 5, 6)))
    expectMsg(Right(Event5("c", true, "d", 5, 6)))
    expectMsg(Right(Event5("e", true, "f", 5, 6)))
    expectMsg(Right(Event5("a", true, "b", 7, 8)))
    expectMsg(Right(Event5("c", true, "d", 7, 8)))
    expectMsg(Right(Event5("e", true, "f", 7, 8)))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq: Query2[Int, Int] = stream[Int, Int]("B")
    val query: Query5[String, Boolean, String, Int, Int] =
      stream[String, Boolean, String]("A")
      .join(sq, tumblingWindow(3 instances), tumblingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Left(GraphCreated))
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
    expectMsg(Right(Event5("a", true, "b", 5, 6)))
    expectMsg(Right(Event5("a", true, "b", 7, 8)))
    expectMsg(Right(Event5("c", true, "d", 5, 6)))
    expectMsg(Right(Event5("c", true, "d", 7, 8)))
    expectMsg(Right(Event5("e", true, "f", 5, 6)))
    expectMsg(Right(Event5("e", true, "f", 7, 8)))
    stopActors(a, b, graph)
  }

  test ("BinaryNode - JoinNode - 3") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq: Query2[Int, Int] = stream[Int, Int]("B")
    val query: Query5[String, Boolean, String, Int, Int] =
      stream[String, Boolean, String]("A")
      .join(sq, slidingWindow(3 instances), slidingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Left(GraphCreated))
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
    expectMsg(Right(Event5("e", true, "f", 1, 2)))
    expectMsg(Right(Event5("g", true, "h", 1, 2)))
    expectMsg(Right(Event5("i", true, "j", 1, 2)))
    expectMsg(Right(Event5("e", true, "f", 3, 4)))
    expectMsg(Right(Event5("g", true, "h", 3, 4)))
    expectMsg(Right(Event5("i", true, "j", 3, 4)))
    expectMsg(Right(Event5("e", true, "f", 5, 6)))
    expectMsg(Right(Event5("g", true, "h", 5, 6)))
    expectMsg(Right(Event5("i", true, "j", 5, 6)))
    expectMsg(Right(Event5("e", true, "f", 7, 8)))
    expectMsg(Right(Event5("g", true, "h", 7, 8)))
    expectMsg(Right(Event5("i", true, "j", 7, 8)))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 4") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq: Query2[Int, Int] = stream[Int, Int]("B")
    val query: Query5[String, Boolean, String, Int, Int] =
      stream[String, Boolean, String]("A")
      .join(sq, slidingWindow(3 instances), slidingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(Left(GraphCreated))
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
    expectMsg(Right(Event5("a", true, "b", 5, 6)))
    expectMsg(Right(Event5("a", true, "b", 7, 8)))
    expectMsg(Right(Event5("c", true, "d", 5, 6)))
    expectMsg(Right(Event5("c", true, "d", 7, 8)))
    expectMsg(Right(Event5("e", true, "f", 5, 6)))
    expectMsg(Right(Event5("e", true, "f", 7, 8)))
    expectMsg(Right(Event5("g", true, "h", 5, 6)))
    expectMsg(Right(Event5("g", true, "h", 7, 8)))
    expectMsg(Right(Event5("i", true, "j", 5, 6)))
    expectMsg(Right(Event5("i", true, "j", 7, 8)))
    stopActors(a, b, graph)
  }

  test("Complex") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val c: ActorRef = getTestPublisher("C")
    val sq1: Query2[String, String] = stream[String, String]("A")
    val sq2: Query2[Int, Int] = stream[Int, Int]("B")
    val sq3: Query1[String] = stream[String]("C")
    val sq4: Query4[String, String, Int, Int] =
      sq1.join(sq2, tumblingWindow(3 instances), tumblingWindow(2 instances))
    val sq5: Query2[String, String] =
      sq3.selfJoin(tumblingWindow(3 instances), tumblingWindow(2 instances))
    val sq6: Query6[String, String, Int, Int, String, String] =
      sq4.join(sq5, tumblingWindow(1 instances), tumblingWindow(4 instances))
    val sq7: Query6[String, String, Int, Int, String, String] =
      sq6.where((_, _, e3, e4, _, _) => e3 < e4)
    val query: Query2[String, String] =
      sq7
      .removeElement2()
      .removeElement2()
      .removeElement2()
      .removeElement2()
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(Left(GraphCreated))
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
    expectMsg(Right(Event2("e", "a")))
    expectMsg(Right(Event2("e", "b")))
    expectMsg(Right(Event2("e", "a")))
    expectMsg(Right(Event2("e", "b")))
    stopActors(a, b, c, graph)
  }

}
