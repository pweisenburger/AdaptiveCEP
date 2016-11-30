package com.scalarookie.eventscala.graph

import akka.actor.{ActorContext, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._

object Node {

  def createChildNodeFrom(query: Query, parentNodeName: String,
                          childNodeId: Int,
                          publishers: Map[String, ActorRef],
                          context: ActorContext): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamNode(stream, publishers)),
      s"$parentNodeName-$childNodeId-stream")
    case join: Join if join.subquery1 == join.subquery2 => context.actorOf(Props(
      new SelfJoinNode(join, publishers)),
      s"$parentNodeName-$childNodeId-join")
    case join: Join if join.subquery1 != join.subquery2 => context.actorOf(Props(
      new JoinNode(join, publishers)),
      s"$parentNodeName-$childNodeId-join")
    case select: Select => context.actorOf(Props(
      new SelectNode(select, publishers)),
      s"$parentNodeName-$childNodeId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterNode(filter, publishers)),
      s"$parentNodeName-$childNodeId-filter")
  }

}
