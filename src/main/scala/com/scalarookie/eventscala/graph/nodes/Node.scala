package com.scalarookie.eventscala.graph.nodes

import akka.actor.{Actor, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.graph.qos._

abstract class Node(publishers: Map[String, ActorRef]) extends Actor {

  val nodeName: String = self.path.name

  def createChildNode(
      query: Query,
      frequencyStrategy: StrategyFactory,
      latencyStrategy: StrategyFactory,
      childNodeId: Int): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamNode(stream, frequencyStrategy, latencyStrategy, publishers)),
      s"$nodeName-$childNodeId-stream")
    case select: Select => context.actorOf(Props(
      new SelectNode(select, frequencyStrategy, latencyStrategy, publishers)),
      s"$nodeName-$childNodeId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterNode(filter, frequencyStrategy, latencyStrategy, publishers)),
      s"$nodeName-$childNodeId-filter")
    case selfJoin: SelfJoin => context.actorOf(Props(
      new SelfJoinNode(selfJoin, frequencyStrategy, latencyStrategy, publishers)),
      s"$nodeName-$childNodeId-selfjoin")
    case join: Join => context.actorOf(Props(
      new JoinNode(join, frequencyStrategy, latencyStrategy, publishers)),
      s"$nodeName-$childNodeId-join")
  }

}
