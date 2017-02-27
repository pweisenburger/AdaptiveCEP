package com.lambdarookie.eventscala.graph.nodes.traits

import akka.actor.{Actor, ActorRef, Props}
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.nodes._
import com.lambdarookie.eventscala.graph.qos._

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
    case dropElemQuery: DropElemQuery =>
      context.actorOf(Props(
        DropElemNode(
          dropElemQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-dropelem")
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

  }

}
