package com.scalarookie.eventscala

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{TestKit, TestProbe}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.dsl._
import com.scalarookie.eventscala.graph.factory.GraphFactory
import com.scalarookie.eventscala.graph.publishers.TestPublisher
import com.scalarookie.eventscala.graph.qos.DummyStrategyFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class GraphTests() extends TestKit(ActorSystem()) with FunSuiteLike with BeforeAndAfterAll {

  def getTestPublisher(name: String): ActorRef =
    system.actorOf(Props(TestPublisher()), name)

  def getTestGraph(query: Query, publishers: Map[String, ActorRef], testActor: ActorRef): ActorRef = GraphFactory(
    query,
    event => testActor ! event,
    DummyStrategyFactory(),
    DummyStrategyFactory(),
    publishers)(system)

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
    val query: Query = stream [String] from "A"
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event1[String]("42")
    expectMsg(Event1[String]("42"))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [Integer, Integer] from "A"
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event2[Integer, Integer](42, 42)
    expectMsg(Event2[Integer, Integer](42, 42))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 3") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Long, java.lang.Long, java.lang.Long] from "A"
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event3[java.lang.Long, java.lang.Long, java.lang.Long](42l, 42l, 42l)
    expectMsg(Event3[java.lang.Long, java.lang.Long, java.lang.Long](42l, 42l, 42l))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 4") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float] from "A"
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event4[java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float](42f, 42f, 42f, 42f)
    expectMsg(Event4[java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float](42f, 42f, 42f, 42f))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 5") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double] from "A"
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event5[java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double](42.0, 42.0, 42.0, 42.0, 42.0)
    expectMsg(Event5[java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double](42.0, 42.0, 42.0, 42.0, 42.0))
    stopActors(a, graph)
  }

  test("LeafNode - StreamNode - 6") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean] from "A"
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event6[java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean](true, true, true, true, true, true)
    expectMsg(Event6[java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean](true, true, true, true, true, true))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [Integer, Integer] from "A" where element(1) >= element(2)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event2[Integer, Integer](41, 42)
    a ! Event2[Integer, Integer](42, 42)
    a ! Event2[Integer, Integer](43, 42)
    expectMsg(Event2[Integer, Integer](42, 42))
    expectMsg(Event2[Integer, Integer](43, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [Integer, Integer] from "A" where element(1) <= element(2)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event2[Integer, Integer](41, 42)
    a ! Event2[Integer, Integer](42, 42)
    a ! Event2[Integer, Integer](43, 42)
    expectMsg(Event2[Integer, Integer](41, 42))
    expectMsg(Event2[Integer, Integer](42, 42))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 3") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Long] from "A" where element(1) === literal(42l)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event1[java.lang.Long](41l)
    a ! Event1[java.lang.Long](42l)
    expectMsg(Event1[java.lang.Long](42l))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 4") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Float] from "A" where element(1) > literal(41f)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event1[java.lang.Float](41f)
    a ! Event1[java.lang.Float](42f)
    expectMsg(Event1[java.lang.Float](42f))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 5") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Double] from "A" where element(1) < literal(42.0)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event1[java.lang.Double](41.0)
    a ! Event1[java.lang.Double](42.0)
    expectMsg(Event1[java.lang.Double](41.0))
    stopActors(a, graph)
  }

  test("UnaryNode - FilterNode - 6") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [java.lang.Boolean] from "A" where element(1) =!= literal(true)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event1[java.lang.Boolean](true)
    a ! Event1[java.lang.Boolean](false)
    expectMsg(Event1[java.lang.Boolean](false))
    stopActors(a, graph)
  }

  test("UnaryNode - SelectNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [Integer, Integer] from "A" select elements(1)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event2[Integer, Integer](21, 42)
    a ! Event2[Integer, Integer](42, 21)
    expectMsg(Event1[Integer](21))
    expectMsg(Event1[Integer](42))
    stopActors(a, graph)
  }

  test("UnaryNode - SelectNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query = stream [String, String, String, String] from "A" select elements(2, 4)
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event4[String, String, String, String]("a", "b", "c", "d")
    a ! Event4[String, String, String, String]("e", "f", "g", "h")
    expectMsg(Event2[String, String]("b", "d"))
    expectMsg(Event2[String, String]("f", "h"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val query: Query =
      stream[String, String].from("A").selfJoin.in(tumblingWindow(3 instances), tumblingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event2[String, String]("a", "b")
    a ! Event2[String, String]("c", "d")
    a ! Event2[String, String]("e", "f")
    expectMsg(Event4[String, String, String, String]("a", "b", "a", "b"))
    expectMsg(Event4[String, String, String, String]("a", "b", "c", "d"))
    expectMsg(Event4[String, String, String, String]("c", "d", "a", "b"))
    expectMsg(Event4[String, String, String, String]("c", "d", "c", "d"))
    expectMsg(Event4[String, String, String, String]("e", "f", "a", "b"))
    expectMsg(Event4[String, String, String, String]("e", "f", "c", "d"))
    stopActors(a, graph)
  }

  test("UnaryNode - SelfJoinNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val sq = stream [String, String] from "A"
    val query: Query = sq.selfJoin.in(slidingWindow(3 instances), slidingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a), testActor)
    expectMsg(GraphCreated)
    a ! Event2[String, String]("a", "b")
    a ! Event2[String, String]("c", "d")
    a ! Event2[String, String]("e", "f")
    expectMsg(Event4[String, String, String, String]("a", "b", "a", "b"))
    expectMsg(Event4[String, String, String, String]("c", "d", "a", "b"))
    expectMsg(Event4[String, String, String, String]("c", "d", "c", "d"))
    expectMsg(Event4[String, String, String, String]("a", "b", "c", "d"))
    expectMsg(Event4[String, String, String, String]("e", "f", "c", "d"))
    expectMsg(Event4[String, String, String, String]("e", "f", "e", "f"))
    expectMsg(Event4[String, String, String, String]("a", "b", "e", "f"))
    expectMsg(Event4[String, String, String, String]("c", "d", "e", "f"))
    stopActors(a, graph)
  }

  test("BinaryNode - JoinNode - 1") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq1 = stream [String, java.lang.Boolean, String] from "A"
    val sq2 = stream [Integer, Integer] from "B"
    val query: Query = sq1 join sq2 in (tumblingWindow(3 instances), tumblingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(GraphCreated)
    a ! Event3[String, java.lang.Boolean, String]("a", true, "b")
    a ! Event3[String, java.lang.Boolean, String]("c", true, "d")
    a ! Event3[String, java.lang.Boolean, String]("e", true, "f")
    a ! Event3[String, java.lang.Boolean, String]("g", true, "h")
    a ! Event3[String, java.lang.Boolean, String]("i", true, "j")
    Thread.sleep(2000)
    b ! Event2[Integer, Integer](1, 2)
    b ! Event2[Integer, Integer](3, 4)
    b ! Event2[Integer, Integer](5, 6)
    b ! Event2[Integer, Integer](7, 8)
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 1, 2))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 1, 2))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 1, 2))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 3, 4))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 3, 4))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 3, 4))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 2") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq1 = stream [String, java.lang.Boolean, String] from "A"
    val sq2 = stream [Integer, Integer] from "B"
    val query: Query = sq1 join sq2 in (tumblingWindow(3 instances), tumblingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(GraphCreated)
    b ! Event2[Integer, Integer](1, 2)
    b ! Event2[Integer, Integer](3, 4)
    b ! Event2[Integer, Integer](5, 6)
    b ! Event2[Integer, Integer](7, 8)
    Thread.sleep(2000)
    a ! Event3[String, java.lang.Boolean, String]("a", true, "b")
    a ! Event3[String, java.lang.Boolean, String]("c", true, "d")
    a ! Event3[String, java.lang.Boolean, String]("e", true, "f")
    a ! Event3[String, java.lang.Boolean, String]("g", true, "h")
    a ! Event3[String, java.lang.Boolean, String]("i", true, "j")
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 7, 8))
    stopActors(a, b, graph)
  }

  test ("BinaryNode - JoinNode - 3") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq1 = stream [String, java.lang.Boolean, String] from "A"
    val sq2 = stream [Integer, Integer] from "B"
    val query: Query = sq1 join sq2 in (slidingWindow(3 instances), slidingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(GraphCreated)
    a ! Event3[String, java.lang.Boolean, String]("a", true, "b")
    a ! Event3[String, java.lang.Boolean, String]("c", true, "d")
    a ! Event3[String, java.lang.Boolean, String]("e", true, "f")
    a ! Event3[String, java.lang.Boolean, String]("g", true, "h")
    a ! Event3[String, java.lang.Boolean, String]("i", true, "j")
    Thread.sleep(2000)
    b ! Event2[Integer, Integer](1, 2)
    b ! Event2[Integer, Integer](3, 4)
    b ! Event2[Integer, Integer](5, 6)
    b ! Event2[Integer, Integer](7, 8)
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 1, 2))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("g", true, "h", 1, 2))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("i", true, "j", 1, 2))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 3, 4))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("g", true, "h", 3, 4))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("i", true, "j", 3, 4))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("g", true, "h", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("i", true, "j", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("g", true, "h", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("BinaryNode - JoinNode - 4") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val sq1 = stream [String, java.lang.Boolean, String] from "A"
    val sq2 = stream [Integer, Integer] from "B"
    val query: Query = sq1 join sq2 in (slidingWindow(3 instances), slidingWindow(2 instances))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b), testActor)
    expectMsg(GraphCreated)
    b ! Event2[Integer, Integer](1, 2)
    b ! Event2[Integer, Integer](3, 4)
    b ! Event2[Integer, Integer](5, 6)
    b ! Event2[Integer, Integer](7, 8)
    Thread.sleep(2000)
    a ! Event3[String, java.lang.Boolean, String]("a", true, "b")
    a ! Event3[String, java.lang.Boolean, String]("c", true, "d")
    a ! Event3[String, java.lang.Boolean, String]("e", true, "f")
    a ! Event3[String, java.lang.Boolean, String]("g", true, "h")
    a ! Event3[String, java.lang.Boolean, String]("i", true, "j")
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("a", true, "b", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("c", true, "d", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("e", true, "f", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("g", true, "h", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("g", true, "h", 7, 8))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("i", true, "j", 5, 6))
    expectMsg(Event5[String, java.lang.Boolean, String, Integer, Integer]("i", true, "j", 7, 8))
    stopActors(a, b, graph)
  }

  test("Complex") {
    val a: ActorRef = getTestPublisher("A")
    val b: ActorRef = getTestPublisher("B")
    val c: ActorRef = getTestPublisher("C")
    val sq1 = stream [String, String] from "A"
    val sq2 = stream [Integer, Integer] from "B"
    val sq3 = stream [String] from "C"
    val sq4 = sq1.join(sq2).in(tumblingWindow(3 instances), tumblingWindow(2 instances))
    val sq5 = sq3.selfJoin.in(tumblingWindow(3 instances), tumblingWindow(2 instances))
    val sq6 = sq4.join(sq5).in(tumblingWindow(1 instances), tumblingWindow(4 instances))
    val sq7 = sq6.where(element(3) < element(4))
    val query: Query = sq7.select(elements(1, 6))
    val graph: ActorRef = getTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), testActor)
    expectMsg(GraphCreated)
    b ! Event2[Integer, Integer](1, 2)
    b ! Event2[Integer, Integer](3, 4)
    b ! Event2[Integer, Integer](5, 6)
    b ! Event2[Integer, Integer](7, 8)
    Thread.sleep(2000)
    a ! Event2[String, String]("a", "b")
    a ! Event2[String, String]("c", "d")
    a ! Event2[String, String]("e", "f")
    a ! Event2[String, String]("g", "h")
    a ! Event2[String, String]("i", "j")
    Thread.sleep(2000)
    c ! Event1[String]("a")
    c ! Event1[String]("b")
    c ! Event1[String]("c")
    expectMsg(Event2[String, String]("e", "a"))
    expectMsg(Event2[String, String]("e", "b"))
    expectMsg(Event2[String, String]("e", "a"))
    expectMsg(Event2[String, String]("e", "b"))
    stopActors(a, b, c, graph)
  }

}
