package com.scalarookie.eventscalaTS.simulation

import akka.actor.{ActorRef, ActorSystem, Props}
import com.scalarookie.eventscalaTS.data.Events._
import com.scalarookie.eventscalaTS.data.Queries._
import com.scalarookie.eventscalaTS.dsl.Dsl._
import com.scalarookie.eventscalaTS.graph.nodes._
import com.scalarookie.eventscalaTS.graph.publishers._

object Main extends App {

  val actorSystem: ActorSystem = ActorSystem()

  val publisherA: ActorRef = actorSystem.actorOf(Props(TestPublisher()), "A")

  val query: Query1[Int] =
    stream[Int, Int]("A", None, None)
    .keepEventsWith(event => event.e1 != event.e2, None, None)
    .removeElement1(None, None)

  val graph: ActorRef = actorSystem.actorOf(Props(SelectNode(
    query.asInstanceOf[SelectQuery],
    Map("A" -> publisherA),
    Some(println))),
    "select")

  Thread.sleep(2000)

  publisherA ! Event2(41, 42)
  publisherA ! Event2(42, 42)
  publisherA ! Event2(43, 42)

}
