package com.scalarookie.eventscala.demos

import akka.actor.{ActorSystem, Props}
import com.scalarookie.eventscala.actors._
import com.scalarookie.eventscala.caseclasses._

object Main extends App {

  val actorSystem = ActorSystem()

  val publisherA = actorSystem.actorOf(Props(RandomEventPublisher("A", id => Event2[Integer, String](id, "X"))), "A")
  val publisherB = actorSystem.actorOf(Props(RandomEventPublisher("B", id => Event2[Integer, Character](id, 'Y'))), "B")
  val publisherC = actorSystem.actorOf(Props(RandomEventPublisher("C", id => Event1[Integer](id))), "C")

  val publishers = Map("A" -> publisherA, "B" -> publisherB, "C" -> publisherC)

  val join: Join = Join(
    Stream2[Integer, String]("A"), Length(3),
    Join(
      Stream2[Integer, Character]("B"), Length(3),
      Stream1[Integer]("C"), Length(3)),
    Length(3))

  val joinActor = actorSystem.actorOf(Props(new JoinActor(join, publishers, None)), "root")

}
