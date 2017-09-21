package com.lambdarookie.eventscala.graph.factory

import akka.actor.{ActorRef, ActorSystem}
import com.lambdarookie.eventscala.backend.system.CentralScheduler
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.monitors._
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}

object GraphFactory {

  def createImpl(
                  system: System,
                  actorSystem: ActorSystem,
                  query: Query,
                  publishers: Map[String, ActorRef],
                  centralScheduler: CentralScheduler,
                  monitors: Set[_ <: Monitor],
                  createdCallback: () => Any,
                  eventCallback: (Event) => Any): ActorRef = {
    centralScheduler.run(system, actorSystem)
    NodeFactory.createNode(system, actorSystem, query, system.createOperator(Operator.ROOT, query, Set.empty[Operator]),
      publishers, monitors, Some(createdCallback), Some(eventCallback), "")
  }

  // This is why `eventCallback` is listed separately:
  // https://stackoverflow.com/questions/21147001/why-scala-doesnt-infer-type-from-generic-type-parameters
  def create[A](
                 system: System,
                 actorSystem: ActorSystem,
                 query: Query1[A],
                 publishers: Map[String, ActorRef],
                 centralScheduler: CentralScheduler,
                 monitors: Set[_ <: Monitor],
                 createdCallback: () => Any)(
      eventCallback: (A) => Any): ActorRef =
    createImpl(
      system,
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      centralScheduler,
      monitors,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B](
                    system: System,
                    actorSystem: ActorSystem,
                    query: Query2[A, B],
                    publishers: Map[String, ActorRef],
                    centralScheduler: CentralScheduler,
                    monitors: Set[_ <: Monitor],
                    createdCallback: () => Any)(
      eventCallback: (A, B) => Any): ActorRef =
    createImpl(
      system,
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      centralScheduler,
      monitors,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C](
                       system: System,
                       actorSystem: ActorSystem,
                       query: Query3[A, B, C],
                       publishers: Map[String, ActorRef],
                       centralScheduler: CentralScheduler,
                       monitors: Set[_ <: Monitor],
                       createdCallback: () => Any)(
      eventCallback: (A, B, C) => Any): ActorRef =
    createImpl(
      system,
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      centralScheduler,
      monitors,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D](
                          system: System,
                          actorSystem: ActorSystem,
                          query: Query4[A, B, C, D],
                          publishers: Map[String, ActorRef],
                          centralScheduler: CentralScheduler,
                          monitors: Set[_ <: Monitor],
                          createdCallback: () => Any)(
      eventCallback: (A, B, C, D) => Any): ActorRef =
    createImpl(
      system,
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      centralScheduler,
      monitors,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D, E](
                             system: System,
                             actorSystem: ActorSystem,
                             query: Query5[A, B, C, D, E],
                             publishers: Map[String, ActorRef],
                             centralScheduler: CentralScheduler,
                             monitors: Set[_ <: Monitor],
                             createdCallback: () => Any)(
      eventCallback: (A, B, C, D, E) => Any): ActorRef =
    createImpl(
      system,
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      centralScheduler,
      monitors,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D, E, F](
                                system: System,
                                actorSystem: ActorSystem,
                                query: Query6[A, B, C, D, E, F],
                                publishers: Map[String, ActorRef],
                                centralScheduler: CentralScheduler,
                                monitors: Set[_ <: Monitor],
                                createdCallback: () => Any)(
      eventCallback: (A, B, C, D, E, F) => Any): ActorRef =
    createImpl(
      system,
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      centralScheduler,
      monitors,
      createdCallback,
      toFunEventAny(eventCallback))

}
