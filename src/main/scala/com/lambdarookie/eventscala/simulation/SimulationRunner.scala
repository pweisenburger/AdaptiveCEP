package com.lambdarookie.eventscala.simulation

import akka.actor.{ActorSystem, Props}
import com.lambdarookie.eventscala.data.Queries.Query
import com.lambdarookie.eventscala.dsl.Dsl._
import com.lambdarookie.eventscala.publishers.EmptyPublisher
import com.lambdarookie.eventscala.system.System

object SimulationRunner extends App {
  def createSimulation = {
    implicit val actorSystem = ActorSystem()

    val publishers = Map(
      "A" -> actorSystem.actorOf(Props(new EmptyPublisher), "A"),
      "B" -> actorSystem.actorOf(Props(new EmptyPublisher), "B"),
      "C" -> actorSystem.actorOf(Props(new EmptyPublisher), "C"))

    val query: Query =
      stream[Int]("A")
        .join(
          stream[Int]("B"),
          slidingWindow(30.seconds),
          slidingWindow(30.seconds))
        .where(_ <= _)

    val system = new System
    system runQuery (query, publishers, None, None)

    val simulation = new Simulation(system)
    simulation.placeSequentially()

    simulation
  }


  val simulationStatic = createSimulation
  val simulationAdaptive = createSimulation

  simulationStatic.placeOptimizingLatency()
  simulationAdaptive.placeOptimizingLatency()

  println("time-s,latencystatic-ms,latencyadaptive-ms")

  0 to 3000 foreach { step =>
    val time = simulationStatic.currentTime.toSeconds
    val simulationStaticMillis = simulationStatic.measureLatency.toMillis
    val simulationAdaptiveMillis = simulationAdaptive.measureLatency.toMillis

    if ((time % 6) == 0)
      println(Seq(time, simulationStaticMillis, simulationAdaptiveMillis) mkString ",")

    if ((time % 60) == 0 && simulationAdaptiveMillis > 80)
      simulationAdaptive.placeOptimizingLatency()

    simulationStatic.advance()
    simulationAdaptive.advance()
  }
}
