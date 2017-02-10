package com.scalarookie.eventscala.graph.nodes.traits

import akka.actor.{Actor, ActorRef, Props}
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.nodes._
import com.scalarookie.eventscala.graph.qos.MonitorFactory

trait Node extends Actor {

  val name: String = self.path.name

  val query: Query
  val publishers: Map[String, ActorRef]
  val frequencyMonitorFactory: MonitorFactory
  val latencyMonitorFactory: MonitorFactory

  def createChildNode(
      id: Int,
      query: Query
    ): ActorRef = query match {
    case streamQuery: StreamQuery =>
      context.actorOf(Props(
        StreamNode(
          streamQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-stream")
    case filterQuery: FilterQuery =>
      context.actorOf(Props(
        FilterNode(
          filterQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-filter")
    case selectQuery: SelectQuery =>
      context.actorOf(Props(
        SelectNode(
          selectQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-select")
    case selfJoinQuery: SelfJoinQuery =>
      context.actorOf(Props(
        SelfJoinNode(
          selfJoinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-selfjoin")
    case joinQuery: JoinQuery =>
      context.actorOf(Props(
        JoinNode(
          joinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-join")
    case conjunctionQuery: ConjunctionQuery =>
      context.actorOf(Props(
        ConjunctionNode(
          conjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-conjunction")
    case disjunctionQuery: DisjunctionQuery =>
      context.actorOf(Props(
        DisjunctionNode(
          disjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-disjunction")
    case sequenceQuery: SequenceQuery =>
      context.actorOf(Props(
        SequenceNode(
          sequenceQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-sequence")
  }

}
