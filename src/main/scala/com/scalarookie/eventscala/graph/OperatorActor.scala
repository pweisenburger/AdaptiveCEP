package com.scalarookie.eventscala.graph

import akka.actor.{ActorContext, ActorRef, Props}
import com.scalarookie.eventscala.caseclasses._

object OperatorActor {

  def createChildActorFrom(
      query: Query,
      parentActorName: String,
      childActorId: Int,
      context: ActorContext,
      publishers: Map[String, ActorRef],
      root: Option[ActorRef]): ActorRef = query match {
    case stream: Stream => context.actorOf(Props(
      new StreamActor(stream, publishers, root)),
      s"$parentActorName-$childActorId-stream")
    case join: Join if join.subquery1 == join.subquery2 => context.actorOf(Props(
      new SelfJoinActor(join, publishers, root)),
      s"$parentActorName-$childActorId-join")
    case join: Join if join.subquery1 != join.subquery2 => context.actorOf(Props(
      new JoinActor(join, publishers, root)),
      s"$parentActorName-$childActorId-join")
    case select: Select => context.actorOf(Props(
      new SelectActor(select, publishers, root)),
      s"$parentActorName-$childActorId-select")
    case filter: Filter => context.actorOf(Props(
      new FilterActor(filter, publishers, root)),
      s"$parentActorName-$childActorId-filter")
  }

}
