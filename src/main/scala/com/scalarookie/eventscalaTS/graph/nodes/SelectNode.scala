package com.scalarookie.eventscalaTS.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscalaTS.data.Events._
import com.scalarookie.eventscalaTS.data.Queries._

case class SelectNode(
    query: SelectQuery,
    //frequencyStrategyFactory: StrategyFactory,
    //latencyStrategyFactory: StrategyFactory,
    publishers: Map[String, ActorRef],
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node {

  val childNode: ActorRef = createChildActor(1, query.sq, publishers)

  val elementToBeRemoved: Int = query match {
    case RemoveElement1Of2(_, _, _) => 1
    case RemoveElement1Of3(_, _, _) => 1
    case RemoveElement1Of4(_, _, _) => 1
    case RemoveElement1Of5(_, _, _) => 1
    case RemoveElement1Of6(_, _, _) => 1
    case RemoveElement2Of2(_, _, _) => 2
    case RemoveElement2Of3(_, _, _) => 2
    case RemoveElement2Of4(_, _, _) => 2
    case RemoveElement2Of5(_, _, _) => 2
    case RemoveElement2Of6(_, _, _) => 2
    case RemoveElement3Of3(_, _, _) => 3
    case RemoveElement3Of4(_, _, _) => 3
    case RemoveElement3Of5(_, _, _) => 3
    case RemoveElement3Of6(_, _, _) => 3
    case RemoveElement4Of4(_, _, _) => 4
    case RemoveElement4Of5(_, _, _) => 4
    case RemoveElement4Of6(_, _, _) => 4
    case RemoveElement5Of5(_, _, _) => 5
    case RemoveElement5Of6(_, _, _) => 5
    case RemoveElement6Of6(_, _, _) => 6
  }

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
  }

  def handleEvent2[A, B](e1: A, e2: B): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event1[B](e2))
    case 2 => emitEvent(Event1[A](e1))
  }

  def handleEvent3[A, B, C](e1: A, e2: B, e3: C): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event2[B, C](e2, e3))
    case 2 => emitEvent(Event2[A, C](e1, e3))
    case 3 => emitEvent(Event2[A, B](e1, e2))
  }

  def handleEvent4[A, B, C, D](e1: A, e2: B, e3: C, e4: D): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event3[B, C, D](e2, e3, e4))
    case 2 => emitEvent(Event3[A, C, D](e1, e3, e4))
    case 3 => emitEvent(Event3[A, B, D](e1, e2, e4))
    case 4 => emitEvent(Event3[A, B, C](e1, e2, e3))
  }

  def handleEvent5[A, B, C, D, E](e1: A, e2: B, e3: C, e4: D, e5: E): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event4[B, C, D, E](e2, e3, e4, e5))
    case 2 => emitEvent(Event4[A, C, D, E](e1, e3, e4, e5))
    case 3 => emitEvent(Event4[A, B, D, E](e1, e2, e4, e5))
    case 4 => emitEvent(Event4[A, B, C, E](e1, e2, e3, e5))
    case 5 => emitEvent(Event4[A, B, C, D](e1, e2, e3, e4))
  }

  def handleEvent6[A, B, C, D, E, F](e1: A, e2: B, e3: C, e4: D, e5: E, e6: F): Unit = elementToBeRemoved match {
    case 1 => emitEvent(Event5[B, C, D, E, F](e2, e3, e4, e5, e6))
    case 2 => emitEvent(Event5[A, C, D, E, F](e1, e3, e4, e5, e6))
    case 3 => emitEvent(Event5[A, B, D, E, F](e1, e2, e4, e5, e6))
    case 4 => emitEvent(Event5[A, B, C, E, F](e1, e2, e3, e5, e6))
    case 5 => emitEvent(Event5[A, B, C, D, F](e1, e2, e3, e4, e6))
    case 6 => emitEvent(Event5[A, B, C, D, E](e1, e2, e3, e4, e5))
  }

  override def receive: Receive = {
    case GraphCreated if sender() == childNode =>
      if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
    case event: Event if sender() == childNode => event match {
      case Event2(e1, e2) => handleEvent2(e1, e2)
      case Event3(e1, e2, e3) => handleEvent3(e1, e2, e3)
      case Event4(e1, e2, e3, e4) => handleEvent4(e1, e2, e3, e4)
      case Event5(e1, e2, e3, e4, e5) => handleEvent5(e1, e2, e3, e4, e5)
      case Event6(e1, e2, e3, e4, e5, e6) => handleEvent6(e1, e2, e3, e4, e5, e6)
    }
  }

}
