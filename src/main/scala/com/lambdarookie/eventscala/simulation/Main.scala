package com.lambdarookie.eventscala.simulation

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.dsl.Dsl._
import com.lambdarookie.eventscala.backend.graph.factory._
import com.lambdarookie.eventscala.backend.graph.monitors._
import com.lambdarookie.eventscala.backend.qos.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.PathFinding.Priority
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.CentralScheduler
import com.lambdarookie.eventscala.backend.system.traits.Host

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object Main extends App {

  val actorSystem: ActorSystem = ActorSystem()
  private val testSystem: TestSystem = TestSystem(Strategies.okStrategy, Priority(1, 1, 1), logging = true)

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))), "D")
  val publisherR1: ActorRef = actorSystem.actorOf(Props(RandomPublisher(i => Event1(Random.nextInt(i + 1) - i / 2))), "R1")
  val publisherR2: ActorRef = actorSystem.actorOf(Props(RandomPublisher(i => Event1(Random.nextInt(i + 1) - i / 2))), "R2")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC,
    "D" -> publisherD,
    "R1" -> publisherR1,
    "R2" -> publisherR2)

  val query1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
    stream[Int]("A")
    .join(
      stream[Int]("B"),
      slidingWindow(2.sec),
      slidingWindow(2.sec),
      latency < 1.ms,
      bandwidth > 60.mbps,
      throughput > 20.mbps)
    .where(
      _ < _,
      latency < 2.ms,
      bandwidth > 59.mbps,
      throughput > 19.mbps)
    .dropElem1(
      latency < 3.ms,
      bandwidth > 58.mbps,
      throughput > 18.mbps)
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      latency < 4.ms,
      bandwidth > 57.mbps,
      throughput > 17.mbps)
    .and(
      stream[Float]("C"),
      latency < 5.ms,
      bandwidth > 56.mbps,
      throughput > 16.mbps)
    .or(
      stream[String]("D"),
      latency < 5.ms,
      bandwidth > 55.mbps,
      throughput > 15.mbps)

  val query2: Query4[Int, Int, Float, String] =
    stream[Int]("A")
    .and(stream[Int]("B"))
    .join(
      sequence(
        nStream[Float]("C") -> nStream[String]("D")),
      slidingWindow(3.sec),
      slidingWindow(3.sec),
      latency < 1.ms)

  val query3: Query3[Int, Int, String] =
    stream[Int]("A")
      .and(
        stream[Int]("B"),
        latency < 10.ms,
        throughput > 40.mbps,
        bandwidth > 70.mbps)
      .join(
        stream[String]("C"),
        tumblingWindow(1.instances),
        tumblingWindow(1.instances),
        bandwidth > 60.mbps,
        throughput > 20.mbps,
        latency < 12.ms)

  val testQuery: Query2[Int, Int] =
    stream[Int]("R1")
      .join(
        stream[Int]("R2"),
        slidingWindow(30.sec),
        slidingWindow(30.sec))
      .where(_ > _)
      .where(_ * _ > 0,
        latency < 20.ms,
        bandwidth > (70.mbps, frequency > Ratio(2.instances, 10.sec)))


  GraphFactory.create(
    system =                  testSystem,
    actorSystem =             actorSystem,
    query =                   testQuery,
    publishers =              publishers,
    centralScheduler =        CentralScheduler( initialDelay = 0,
                                                latencyInterval = 10, bandwidthInterval = 10, throughputInterval = 0),
    monitors =                Set(ConditionsMonitor (frequencyInterval = 10, proximityInterval = 0, logging = true),
                                  DemandsMonitor    (interval = 3, logging = true)),
    createdCallback =         () => println("STATUS:\t\tGraph has been created."))(
    eventCallback =           {
      // Callback for `query1`:
//      case (Left(i1), Left(i2), Left(f))  => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
//      case (Right(s), _, _)               => println(s"COMPLEX EVENT:\tEvent1($s)")
      // Callback for `query2`:
//      case (i1, i2, f, s)                 => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f,$s)")
      // Callback for `query3`:
//      case (i1, i2, s)                    => println(s"COMPLEX EVENT:\tEvent3($i1, $i2, $s)")
      // Callback for `testQuery`:
      case (i1, i2)                       => println(s"COMPLEX EVENT:\tEvent2($i1, $i2)")
      // This is necessary to avoid warnings about non-exhaustive `match`:
      case _                              =>
    })


  actorSystem.scheduler.scheduleOnce(FiniteDuration(20, TimeUnit.SECONDS), () => {
    val host2: TestHost = testSystem.getHostById(2).get
    host2.neighbors.foreach(n => host2.neighborLatencies += n -> 15.ms)
  })

  actorSystem.scheduler.scheduleOnce(FiniteDuration(40, TimeUnit.SECONDS), () => {
    val host1: Host = testSystem.getHostById(1).get
    testSystem.removeHosts(Set(host1))
  })

  actorSystem.scheduler.scheduleOnce(FiniteDuration(60, TimeUnit.SECONDS), () => {
    val host2: TestHost = testSystem.getHostById(2).get
    host2.neighbors.foreach(n => host2.neighborLatencies += n -> 5.ms)
  })

}
