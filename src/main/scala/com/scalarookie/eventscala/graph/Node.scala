package com.scalarookie.eventscala.graph

import akka.actor.{ActorContext, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{AverageFrequencyStrategy, PathLatencyBinaryNodeStrategy, PathLatencyLeafNodeStrategy, PathLatencyUnaryNodeStrategy}

object Node {

  def createChildNodeFrom(
       query: Query, parentNodeName: String,
       childNodeId: Int,
       publishers: Map[String, ActorRef],
       context: ActorContext): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamNode(stream, publishers, new AverageFrequencyStrategy(10), new PathLatencyLeafNodeStrategy)),
      s"$parentNodeName-$childNodeId-stream")
    case join: Join if join.subquery1 == join.subquery2 => context.actorOf(Props(
      new SelfJoinNode(join, publishers, new AverageFrequencyStrategy(10), new PathLatencyUnaryNodeStrategy(5))),
      s"$parentNodeName-$childNodeId-join")
    case join: Join if join.subquery1 != join.subquery2 => context.actorOf(Props(
      new JoinNode(join, publishers, new AverageFrequencyStrategy(10), new PathLatencyBinaryNodeStrategy(5))),
      s"$parentNodeName-$childNodeId-join")
    case select: Select => context.actorOf(Props(
      new SelectNode(select, publishers, new AverageFrequencyStrategy(10), new PathLatencyUnaryNodeStrategy(5))),
      s"$parentNodeName-$childNodeId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterNode(filter, publishers, new AverageFrequencyStrategy(10), new PathLatencyUnaryNodeStrategy(5))),
      s"$parentNodeName-$childNodeId-filter")
  }

}
