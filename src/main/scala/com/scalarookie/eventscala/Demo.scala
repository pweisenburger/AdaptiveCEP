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
    stream[String, Integer].from("B")
    .select(elements(2))

  val query: Query =
    stream[Integer, String].from("A")
    .join(subquery).in(slidingWindow(3 instances), tumblingWindow(3 seconds))
    .join(stream[java.lang.Boolean].from("C")).in(slidingWindow(1 instances), slidingWindow(1 instances))
    .select(elements(1, 2, 4))
    .where(element(1) <= literal(15))
    .latency(latency <= timespan(1 milliseconds) otherwise {
      nodeName => println(s"LATENCY WARNING:\t$nodeName's latency is higher than 1 millisecond.")})
    .where(literal(true) =!= element(3))
    .frequency(frequency > ratio(3 instances, 5 seconds) otherwise {
      nodeName => println(s"FREQUENCY WARNING:\t$nodeName doesn't on average emit at least 3 instances per 5 seconds.") })

  val query2: Query =
    stream[Integer, String].from("A")
      .selfJoin(stream[Integer, String].from("A")).in(tumblingWindow(1 instances), tumblingWindow(1 instances))

  val query3: Query =
    stream[Integer, String].from("A")
      .frequency(frequency > ratio(3 instances, 5 seconds) otherwise { nodeName => println(s"WARNING:\t\t$nodeName is slow.") })
      .where(element(1) >= literal(1))
      .where(element(1) >= literal(2))
      .select(elements(1))
      .where(element(1) >= literal(3))

  val graph = actorSystem.actorOf(Props(
    new RootNode(query, publishers, event => println(s"COMPLEX EVENT:\t\t$event"))),
    "root")

}
