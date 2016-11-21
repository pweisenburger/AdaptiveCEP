package com.scalarookie.eventscala.demos

import akka.actor.{ActorSystem, Props}
import com.scalarookie.eventscala.actors._
import com.scalarookie.eventscala.caseclasses._

object Main extends App {

  val actorSystem = ActorSystem()

  val publisherA = actorSystem.actorOf(Props(RandomPublisherActor("A", id => Event2[Integer, String](id, "X"))), "A")
  val publisherB = actorSystem.actorOf(Props(RandomPublisherActor("B", id => Event2[Integer, Character](id, 'Y'))), "B")
  val publisherC = actorSystem.actorOf(Props(RandomPublisherActor("C", id => Event1[Integer](id))), "C")

  val publishers = Map("A" -> publisherA, "B" -> publisherB, "C" -> publisherC)

  val join1: Join = Join(
    Stream2[Integer, String]("A"), LengthTumbling(1),
    Stream2[Integer, Character]("B"), LengthTumbling(1))

  val join2: Join = Join(
    join1, TimeSliding(3),
    Stream2[Integer, String]("A"), TimeSliding(3))

  val select: Select = Select(join2, List(3, 4, 5))

  val selectActor = actorSystem.actorOf(Props(new SelectActor(select, publishers, None)), "select")

}
