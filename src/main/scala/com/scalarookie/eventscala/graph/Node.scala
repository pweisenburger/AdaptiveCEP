package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos._

abstract class Node(publishers: Map[String, ActorRef]) extends Actor {

  val nodeName: String = self.path.name

  def createChildNode(query: Query, childNodeId: Int): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamNode(stream, publishers, new FrequencyLeafNodeStrategy(5), new LatencyLeafNodeStrategy)),
      s"$nodeName-$childNodeId-stream")
    case select: Select => context.actorOf(Props(
      new SelectNode(select, publishers, new FrequencyUnaryNodeStrategy(5), new LatencyUnaryNodeStrategy(10))),
      s"$nodeName-$childNodeId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterNode(filter, publishers, new FrequencyUnaryNodeStrategy(5), new LatencyUnaryNodeStrategy(10))),
      s"$nodeName-$childNodeId-filter")
    case selfJoin: SelfJoin => context.actorOf(Props(
      new SelfJoinNode(selfJoin, publishers, new FrequencyUnaryNodeStrategy(5), new LatencyUnaryNodeStrategy(10))),
      s"$nodeName-$childNodeId-selfjoin")
    case join: Join => context.actorOf(Props(
      new JoinNode(join, publishers, new FrequencyBinaryNodeStrategy(5), new LatencyBinaryNodeStrategy(10))),
      s"$nodeName-$childNodeId-join")
  }

}
