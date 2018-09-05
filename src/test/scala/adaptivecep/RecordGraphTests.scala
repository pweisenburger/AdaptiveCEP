package adaptivecep

import adaptivecep.data.Events.{Created, Event}
import adaptivecep.data.Queries.Query
import adaptivecep.dsl.Dsl._
import akka.actor.ActorRef
import shapeless.{::, HNil, Nat, Witness}
import shapeless.labelled.KeyTag
import shapeless.record.Record
import shapeless.syntax.singleton._

class RecordGraphTests extends GraphTestSuite {
  val wName = Witness("name")
  val wOther = Witness("other")
  val wAge = Witness("age")

  test("Record - JoinNode") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    val sq: Query[Record.`"name" -> Int`.T] = stream[Record.`"name" -> Int`.T]("B")
    val query: Query[Record.`"other" -> Boolean, "name" -> Int`.T] =
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
    val sq: Query[Record.`"name" -> Int`.T] = stream[Record.`"name" -> Int`.T]("B")
    val query: Query[Record.`"other" -> Boolean, "age" -> Int`.T] =
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
    val query: Query[Record.`"name" -> Boolean`.T] =
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
    val query: Query[Int with KeyTag[wOther.T, Int]::HNil] =
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
    val query: Query[Boolean with KeyTag[wName.T, Boolean]::HNil] =
      stream[Boolean with KeyTag[wName.T, Boolean]::Int with KeyTag[wOther.T, Int]::HNil]("A")
        .drop(wOther)
    val graph: ActorRef = createTestGraph(query, Map("A" -> a), testActor)
    expectMsg(Created)
    a ! Event("name" ->> true, "other" ->> 3)
    expectMsg(Event("name" ->> true))
    stopActors(a, graph)
  }

  test("Record - DropElemNode - 3") {
    val a: ActorRef = createTestPublisher("A")
    val query: Query[Record.`"name" -> Boolean, "other" -> Int`.T] =
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
    val query: Query[Record.`"name" -> Boolean, "age" -> Int`.T] =
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
    val query: Query[Record.`"name" -> Boolean, "other" -> String`.T] =
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
    val query: Query[Record.`"name" -> Boolean`.T] =
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
    val query: Query[Record.`"other" -> String`.T] =
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
    val query = // : Query[Boolean with KeyTag[wName.T,  Boolean] :: HNil] =
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
    val query: Query[Either[Boolean with KeyTag[wName.T, Boolean], Int] :: HNil] =
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
    val query = //: Query[Record.`"name" -> Boolean, "other" -> String`.T] =
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
    val query = //: Query[Record.`"name" -> Boolean, "other" -> String`.T] =
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
    val query = //: Query[Record.`"name" -> Boolean, "other" -> String`.T] =
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
}
