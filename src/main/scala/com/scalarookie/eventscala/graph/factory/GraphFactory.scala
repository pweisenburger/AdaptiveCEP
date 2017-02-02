package com.scalarookie.eventscala.graph.factory

import akka.actor.{ActorRef, ActorSystem, Props}
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.nodes._
import com.scalarookie.eventscala.graph.qos._

object GraphFactory {

  def createImpl(
      actorSystem: ActorSystem,
      query: Query,
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any,
      eventCallback: (Event) => Any): ActorRef = query match {
    case streamQuery: StreamQuery =>
      actorSystem.actorOf(Props(
        StreamNode(
          streamQuery, publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "stream")
    case filterQuery: FilterQuery =>
      actorSystem.actorOf(Props(
        FilterNode(
          filterQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "filter")
    case selectQuery: SelectQuery =>
      actorSystem.actorOf(Props(
        SelectNode(
          selectQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "select")
    case selfJoinQuery: SelfJoinQuery =>
      actorSystem.actorOf(Props(
        SelfJoinNode(
          selfJoinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "selfjoin")
    case joinQuery: JoinQuery =>
      actorSystem.actorOf(Props(
        JoinNode(
          joinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "join")
  }

  // Source: https://stackoverflow.com/questions/21147001/why-scala-doesnt-infer-type-from-generic-type-parameters
  def create[A](
      actorSystem: ActorSystem,
      query: Query1[A],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B](
      actorSystem: ActorSystem,
      query: Query2[A, B],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C](
      actorSystem: ActorSystem,
      query: Query3[A, B, C],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D](
      actorSystem: ActorSystem,
      query: Query4[A, B, C, D],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C, D) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D, E](
      actorSystem: ActorSystem,
      query: Query5[A, B, C, D, E],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C, D, E) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D, E, F](
      actorSystem: ActorSystem,
      query: Query6[A, B, C, D, E, F],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C, D, E, F) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

}
