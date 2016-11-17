package com.scalarookie.eventscala.demos

import akka.actor.{ActorSystem, Props}
import com.scalarookie.eventscala.actors._
import com.scalarookie.eventscala.caseclasses._

object Main extends App {

  val actorSystem = ActorSystem()

  // `RandomPublisherActor` is a dummy implementation of the `PublisherActor` interface. It simply takes a function
  // (Int => T) and then starts to randomly publish `T`s that were created using that function.
  val publisherA = actorSystem.actorOf(Props(RandomPublisherActor("A", id => Event2[Integer, String](id, "X"))), "A")
  val publisherB = actorSystem.actorOf(Props(RandomPublisherActor("B", id => Event2[Integer, Character](id, 'Y'))), "B")
  val publisherC = actorSystem.actorOf(Props(RandomPublisherActor("C", id => Event1[Integer](id))), "C")

  val publishers = Map("A" -> publisherA, "B" -> publisherB, "C" -> publisherC)

  // This is the case class representation of the query to be executed. A DSL for nicer syntax will follow! :)
  val join: Join = Join(
    Stream2[Integer, String]("A"), Length(3),
    Join(
      Stream2[Integer, Character]("B"), Length(3),
      Stream1[Integer]("C"), Length(3)),
    Length(3))

  // Creating a `JoinActor` will result in an event graph that executes the given query.
  val joinActor = actorSystem.actorOf(Props(new JoinActor(join, publishers, None)), "root")

}
