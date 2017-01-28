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

  val childNode: ActorRef = createChildNode(1, query.sq, publishers)

  def emitGraphCreated(): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
  }

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
  }

  def handleEvent1(e1: Any): Unit = query match {
    case KeepEventsWith1(_, cond, _, _) =>
      val castedCond: (Any) => Boolean = cond.asInstanceOf[(Any) => Boolean]
      if (castedCond(e1)) emitEvent(Event1(e1))
    case _ => ??? // Simply throw an exception if control flow ever reaches this point -- which should never happen!
  }

  def handleEvent2(e1: Any,  e2: Any): Unit = query match {
    case KeepEventsWith2(_, cond, _, _) =>
      val castedCond: (Any, Any) => Boolean = cond.asInstanceOf[(Any, Any) => Boolean]
      if (castedCond(e1, e2)) emitEvent(Event2(e1, e2))
    case _ => ???
  }

  def handleEvent3(e1: Any,  e2: Any, e3: Any): Unit = query match {
    case KeepEventsWith3(_, cond, _, _) =>
      val castedCond: (Any, Any, Any) => Boolean = cond.asInstanceOf[(Any, Any, Any) => Boolean]
      if (castedCond(e1, e2, e3)) emitEvent(Event3(e1, e2, e3))
    case _ => ???

  }

  def handleEvent4(e1: Any,  e2: Any, e3: Any, e4: Any): Unit = query match {
    case KeepEventsWith4(_, cond, _, _) =>
      val castedCond: (Any, Any, Any, Any) => Boolean = cond.asInstanceOf[(Any, Any, Any, Any) => Boolean]
      if (castedCond(e1, e2, e3, e4)) emitEvent(Event4(e1, e2, e3, e4))
    case _ => ???
  }

  def handleEvent5(e1: Any,  e2: Any, e3: Any, e4: Any, e5: Any): Unit = query match {
    case KeepEventsWith5(_, cond, _, _) =>
      val castedCond: (Any, Any, Any, Any, Any) => Boolean = cond.asInstanceOf[(Any, Any, Any, Any, Any) => Boolean]
      if (castedCond(e1, e2, e3, e4, e5)) emitEvent(Event5(e1, e2, e3, e4, e5))
    case _ => ???
  }

  def handleEvent6(e1: Any,  e2: Any, e3: Any, e4: Any, e5: Any, e6: Any): Unit = query match {
    case KeepEventsWith6(_, cond, _, _) =>
      val castedCond: (Any, Any, Any, Any, Any, Any) => Boolean = cond.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Boolean]
      if (castedCond(e1, e2, e3, e4, e5, e6)) emitEvent(Event6(e1, e2, e3, e4, e5, e6))
    case _ => ???
  }

  override def receive: Receive = {
    case GraphCreated if sender() == childNode =>
      emitGraphCreated()
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
