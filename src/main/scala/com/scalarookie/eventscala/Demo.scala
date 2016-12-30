package com.scalarookie.eventscala

import akka.actor.{ActorSystem, Props}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.dsl._
import com.scalarookie.eventscala.graph._
import com.scalarookie.eventscala.publishers.RandomPublisher

object Demo extends App {
  val actorSystem = ActorSystem()

  val publisherA = actorSystem.actorOf(Props(
    RandomPublisher(id => Event2[Integer, String](id, id.toString))), "A")
  val publisherB = actorSystem.actorOf(Props(
    RandomPublisher(id => Event2[String, Integer](id.toString, id))), "B")
  val publisherC = actorSystem.actorOf(Props(
    RandomPublisher(id => Event1[java.lang.Boolean](if (id % 2 == 0) true else false))), "C")

  val publishers = Map("A" -> publisherA, "B" -> publisherB, "C" -> publisherC)

  val subquery: Query =
    stream[String, Integer].from("B", None)
    .select(elements(2), None, None)

  val query: Query =
    stream[Integer, String].from("A", None)
    .join(subquery, None, None).in(slidingWindow(3 instances), tumblingWindow(3 seconds))
    .join(stream[java.lang.Boolean].from("C", None), None, None).in(slidingWindow(1 instances), slidingWindow(1 instances))
    .select(elements(1, 2, 4), None, None)
    .where(element(1) <= literal(15),
      None,
      Some(latency <= timespan(/*126*/ 0 milliseconds) otherwise { nodeName => println(s"WARNING:\t\t$nodeName sucks tit.") }))
    .where(literal(true) =!= element(3),
      Some(frequency > ratio(4 instances, 5 seconds) otherwise { nodeName => println(s"WARNING:\t\t$nodeName is slow.") }),
      None)

  val query2: Query =
    stream[Integer, String].from("A", None)
      .join(stream[Integer, String].from("A", None), None, None)
      .in(tumblingWindow(1 instances), tumblingWindow(1 instances))

  val query3: Query =
    stream[Integer, String].from("A",
      Some(frequency > ratio(3 instances, 5 seconds) otherwise { nodeName => println(s"WARNING:\t\t$nodeName is slow.") }))
      .where(element(1) >= literal(1), None, None)
      .where(element(1) >= literal(2), None, None)
      .select(elements(1), None, None)
      .where(element(1) >= literal(3), None, None)

  val graph = actorSystem.actorOf(Props(
    new RootNode(query, publishers, event => println(s"COMPLEX EVENT:\t\t$event"))),
    "root")
}
