package com.scalarookie.eventscala.simulation

import akka.actor.{ActorRef, ActorSystem, Props}
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.dsl.Dsl._
import com.scalarookie.eventscala.graph.factory._
import com.scalarookie.eventscala.graph.qos._
import com.scalarookie.eventscala.publishers._

object Main extends App {

  val actorSystem: ActorSystem = ActorSystem()

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))),     "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))), "B")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB)

  val sampleQuery: Query2[Int, Int] =
    stream[Int]("A")
    .join(
      stream[Int]("B"),
      slidingWindow(2.seconds),
      slidingWindow(2.seconds))
    .keepEventsWith(_ < _)
    .removeElement1(
      latency < timespan(1.milliseconds) otherwise { (name) => println(s"PROBLEM:\tEvents reach node `$name` too slow!") })
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      frequency > ratio( 3.instances,  5.seconds) otherwise { (name) => println(s"PROBLEM:\tNode `$name` emits too few events!") },
      frequency < ratio(12.instances, 15.seconds) otherwise { (name) => println(s"PROBLEM:\tNode `$name` emits too many events!") })

  val graph: ActorRef = GraphFactory.create(
    actorSystem =             actorSystem,
    query =                   sampleQuery,
    publishers =              publishers,
    frequencyMonitorFactory = AveragedFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory =   PathLatencyMonitorFactory       (interval =  5, logging = true),
    createdCallback =         () =>       println("STATUS:\tGraph has been created."))(
    eventCallback =           (e1, e2) => println(s"COMPLEX EVENT:\tEvent2($e1,$e2)"))
  
}
