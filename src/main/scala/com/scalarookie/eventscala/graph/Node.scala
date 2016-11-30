package com.scalarookie.eventscala.graph

import akka.actor.{ActorContext, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._

object Node {

  def createChildNodeFrom(
      query: Query,
      parentNodeName: String,
      childNodeId: Int,
      context: ActorContext,
      publishers: Map[String, ActorRef],
      root: Option[ActorRef]): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamNode(stream, publishers, root)),
      s"$parentNodeName-$childNodeId-stream")
    case join: Join if join.subquery1 == join.subquery2 => context.actorOf(Props(
      new SelfJoinNode(join, publishers, root)),
      s"$parentNodeName-$childNodeId-join")
    case join: Join if join.subquery1 != join.subquery2 => context.actorOf(Props(
      new JoinNode(join, publishers, root)),
      s"$parentNodeName-$childNodeId-join")
    case select: Select => context.actorOf(Props(
      new SelectNode(select, publishers, root)),
      s"$parentNodeName-$childNodeId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterNode(filter, publishers, root)),
      s"$parentNodeName-$childNodeId-filter")
  }

}
