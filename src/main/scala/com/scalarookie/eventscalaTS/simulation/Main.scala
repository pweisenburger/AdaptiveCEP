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
  val publisherB: ActorRef = actorSystem.actorOf(Props(TestPublisher()), "B")

  val query: Query2[Int, Int] =
    stream[Int]("A", None, None)
    .join(
      stream[Int]("B", None, None),
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      None, None)
    .keepEventsWith(
      _ > _,
      None, None)
    .removeElement1(None, None)
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      None, None)

  val graph: ActorRef = actorSystem.actorOf(Props(SelfJoinNode(
    query.asInstanceOf[SelfJoinQuery],
    Map("A" -> publisherA, "B" -> publisherB),
    Some(println))),
    "selfjoin")

  Thread.sleep(2000)

  publisherA ! Event1(42)
  publisherB ! Event1(13)
  publisherB ! Event1(21)


}
