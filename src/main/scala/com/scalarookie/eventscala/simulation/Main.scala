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

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))), "C")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC)

  val sampleQuery: Query2[Either[Int, String], Either[Int, Unit]] =
    stream[Int]("A")
    .join(
      stream[Int]("B"),
      slidingWindow(2.seconds),
      slidingWindow(2.seconds))
    .keepEventsWith(_ < _)
    .removeElement1(
      latency < timespan(1.milliseconds) otherwise { (nodeData) => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slow!") })
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      frequency > ratio( 3.instances,  5.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },
      frequency < ratio(12.instances, 15.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") })
    .or(stream[String]("B"))

  val graph: ActorRef = GraphFactory.create(
    actorSystem =             actorSystem,
    query =                   sampleQuery,
    publishers =              publishers,
    frequencyMonitorFactory = AveragedFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory =   PathLatencyMonitorFactory       (interval =  5, logging = true),
    createdCallback =         () => println("STATUS:\tGraph has been created."))(
    eventCallback =           {
      case (Left(i1), Left(i2)) =>  println(s"COMPLEX EVENT:\tEvent2($i1,$i2)")
      case (Right(s), _)        =>  println(s"COMPLEX EVENT:\tEvent1($s)")
      case _ =>                     // This is necessary to avoid warnings about non-exhaustive `match`.
    })
  
}
