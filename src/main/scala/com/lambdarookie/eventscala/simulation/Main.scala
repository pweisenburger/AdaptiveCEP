package com.lambdarookie.eventscala.simulation

import akka.actor.{ActorRef, ActorSystem, Props}
import com.lambdarookie.eventscala.backend.system.TestSystem
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.dsl.Dsl._
import com.lambdarookie.eventscala.graph.factory._
import com.lambdarookie.eventscala.graph.monitors._
import com.lambdarookie.eventscala.publishers._
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._

object Main extends App {

  val actorSystem: ActorSystem = ActorSystem()
  val system: System = new TestSystem

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))), "D")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC,
    "D" -> publisherD)

  val query1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
    stream[Int]("A")
    .join(
      stream[Int]("B"),
      slidingWindow(2.sec),
      slidingWindow(2.sec))
    .where(_ < _)
    .dropElem1(
      latency < (1.ms, frequency > Ratio(10.instances, 5.sec)))
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances))
    .and(stream[Float]("C"))
    .or(stream[String]("D"))

  val query2: Query4[Int, Int, Float, String] =
    stream[Int]("A")
    .and(stream[Int]("B"))
    .join(
      sequence(
        nStream[Float]("C") -> nStream[String]("D")),
      slidingWindow(3.sec),
      slidingWindow(3.sec),
      latency < 1.ms)


  GraphFactory.create(
    system =                  system,
    actorSystem =             actorSystem,
    query =                   query1, // Alternatively: `query2`
    publishers =              publishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory  (interval = 15, logging = true, testing = true),
    latencyMonitorFactory   = PathLatencyMonitorFactory       (interval =  5, logging = true, testing = true),
    createdCallback =         () => println("STATUS:\t\tGraph has been created."))(
    eventCallback =           {
      // Callback for `query1`:
      case (Left(i1), Left(i2), Left(f)) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
      case (Right(s), _, _)              => println(s"COMPLEX EVENT:\tEvent1($s)")
      // Callback for `query2`:
      // case (i1, i2, f, s)             => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f,$s)")
      // This is necessary to avoid warnings about non-exhaustive `match`:
      case _                             =>
    })

}
