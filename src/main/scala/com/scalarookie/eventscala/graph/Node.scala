package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{AverageFrequencyStrategy, PathLatencyBinaryNodeStrategy, PathLatencyLeafNodeStrategy, PathLatencyUnaryNodeStrategy}

abstract class Node(publishers: Map[String, ActorRef]) extends Actor {

  val nodeName: String = self.path.name

  def createChildNode(query: Query, childNodeId: Int): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamNode(stream, publishers, new AverageFrequencyStrategy(10), new PathLatencyLeafNodeStrategy)),
      s"$nodeName-$childNodeId-stream")
    case select: Select => context.actorOf(Props(
      new SelectNode(select, publishers, new AverageFrequencyStrategy(10), new PathLatencyUnaryNodeStrategy(5))),
      s"$nodeName-$childNodeId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterNode(filter, publishers, new AverageFrequencyStrategy(10), new PathLatencyUnaryNodeStrategy(5))),
      s"$nodeName-$childNodeId-filter")
    case selfJoin: SelfJoin => context.actorOf(Props(
      new SelfJoinNode(selfJoin, publishers, new AverageFrequencyStrategy(10), new PathLatencyUnaryNodeStrategy(5))),
      s"$nodeName-$childNodeId-selfjoin")
    case join: Join => context.actorOf(Props(
      new JoinNode(join, publishers, new AverageFrequencyStrategy(10), new PathLatencyBinaryNodeStrategy(5))),
      s"$nodeName-$childNodeId-join")
  }

}
