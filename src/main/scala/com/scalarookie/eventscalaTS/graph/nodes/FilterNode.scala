package com.scalarookie.eventscalaTS.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscalaTS.data.Events._
import com.scalarookie.eventscalaTS.data.Queries._

case class FilterNode(
    query: FilterQuery,
    //frequencyStrategyFactory: StrategyFactory,
    //latencyStrategyFactory: StrategyFactory,
    publishers: Map[String, ActorRef],
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node {

  val childNode: ActorRef = createChildActor(1, query.sq, publishers)

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
  }

  def handleEvent1[A](e1: A): Unit = query match {
    case KeepEventsWith1(_, cond, _, _) => if (cond(Event1(e1))) emitEvent(Event1(e1))
  }

  def handleEvent2[A, B](e1: A, e2: B): Unit = query match {
    case KeepEventsWith2(_, cond, _, _) => if (cond(Event2(e1, e2))) emitEvent(Event2(e1, e2))
  }

  def handleEvent3[A, B, C](e1: A, e2: B, e3: C): Unit = query match {
    case KeepEventsWith3(_, cond, _, _) => if (cond(Event3(e1, e2, e3))) emitEvent(Event3(e1, e2, e3))
  }

  def handleEvent4[A, B, C, D](e1: A, e2: B, e3: C, e4: D): Unit = query match {
    case KeepEventsWith4(_, cond, _, _) => if (cond(Event4(e1, e2, e3, e4))) emitEvent(Event4(e1, e2, e3, e4))
  }

  def handleEvent5[A, B, C, D, E](e1: A, e2: B, e3: C, e4: D, e5: E): Unit = query match {
    case KeepEventsWith5(_, cond, _, _) => if (cond(Event5(e1, e2, e3, e4, e5))) emitEvent(Event5(e1, e2, e3, e4, e5))
  }

  def handleEvent6[A, B, C, D, E, F](e1: A, e2: B, e3: C, e4: D, e5: E, e6: F): Unit = query match {
    case KeepEventsWith6(_, cond, _, _) => if (cond(Event6(e1, e2, e3, e4, e5, e6))) emitEvent(Event6(e1, e2, e3, e4, e5, e6))
  }

  override def receive: Receive = {
    case GraphCreated if sender() == childNode =>
      if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
    case event: Event if sender() == childNode => event match {
      case Event1(e1) => handleEvent1(e1)
      case Event2(e1, e2) => handleEvent2(e1, e2)
      case Event3(e1, e2, e3) => handleEvent3(e1, e2, e3)
      case Event4(e1, e2, e3, e4) => handleEvent4(e1, e2, e3, e4)
      case Event5(e1, e2, e3, e4, e5) => handleEvent5(e1, e2, e3, e4, e5)
      case Event6(e1, e2, e3, e4, e5, e6) => handleEvent6(e1, e2, e3, e4, e5, e6)
    }
  }

}
