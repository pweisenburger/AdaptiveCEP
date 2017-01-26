package com.scalarookie.eventscalaTS.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscalaTS.data.Events._
import com.scalarookie.eventscalaTS.data.Queries._
import com.scalarookie.eventscalaTS.graph.publishers.Publisher._

case class StreamNode(
    query: StreamQuery,
    //frequencyStrategyFactory: StrategyFactory,
    //latencyStrategyFactory: StrategyFactory,
    publishers: Map[String, ActorRef],
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node {

  val publisher: ActorRef = query match {
    case Stream1(publisherName, _, _) => publishers(publisherName)
    case Stream2(publisherName, _, _) => publishers(publisherName)
    case Stream3(publisherName, _, _) => publishers(publisherName)
    case Stream4(publisherName, _, _) => publishers(publisherName)
    case Stream5(publisherName, _, _) => publishers(publisherName)
    case Stream6(publisherName, _, _) => publishers(publisherName)
  }

  publisher ! Subscribe

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
  }

  override def receive: Receive = {
    case AcknowledgeSubscription if sender() == publisher =>
      if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
    case event: Event if sender() == publisher =>
      emitEvent(event)
  }

}
