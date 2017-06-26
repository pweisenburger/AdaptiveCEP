package com.lambdarookie.eventscala.graph.factory

import akka.actor.{ActorRef, ActorRefFactory, Props}
import com.lambdarookie.eventscala.backend.system.{BinaryOperator, EventSource, UnaryOperator}
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.data.Events.Event
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.nodes._
import com.lambdarookie.eventscala.graph.qos.MonitorFactory

/**
  * Created by monur.
  */
object NodeFactory {
  def createNode[T <: ActorRefFactory](system: System,
                 actorRefFactory: T,
                 query: Query,
                 operator: Operator,
                 publishers: Map[String, ActorRef],
                 frequencyMonitorFactory: MonitorFactory,
                 latencyMonitorFactory: MonitorFactory,
                 createdCallback: Option[() => Any],
                 eventCallback: Option[(Event) => Any],
                 prefix: String,
                 testId: String): ActorRef = query match {
    case streamQuery: StreamQuery =>
      actorRefFactory.actorOf(Props(
        StreamNode(
          system,
          streamQuery,
          operator.asInstanceOf[EventSource],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}stream")
    case sequenceQuery: SequenceQuery =>
      actorRefFactory.actorOf(Props(
        SequenceNode(
          system,
          sequenceQuery,
          operator.asInstanceOf[EventSource],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}sequence")
    case filterQuery: FilterQuery =>
      actorRefFactory.actorOf(Props(
        FilterNode(
          system,
          filterQuery,
          operator.asInstanceOf[UnaryOperator],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}filter")
    case dropElemQuery: DropElemQuery =>
      actorRefFactory.actorOf(Props(
        DropElemNode(
          system,
          dropElemQuery,
          operator.asInstanceOf[UnaryOperator],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}dropelem")
    case selfJoinQuery: SelfJoinQuery =>
      actorRefFactory.actorOf(Props(
        SelfJoinNode(
          system,
          selfJoinQuery,
          operator.asInstanceOf[UnaryOperator],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}selfjoin")
    case joinQuery: JoinQuery =>
      actorRefFactory.actorOf(Props(
        JoinNode(
          system,
          joinQuery,
          operator.asInstanceOf[BinaryOperator],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}join")
    case conjunctionQuery: ConjunctionQuery =>
      actorRefFactory.actorOf(Props(
        ConjunctionNode(
          system,
          conjunctionQuery,
          operator.asInstanceOf[BinaryOperator],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}conjunction")
    case disjunctionQuery: DisjunctionQuery =>
      actorRefFactory.actorOf(Props(
        DisjunctionNode(
          system,
          disjunctionQuery,
          operator.asInstanceOf[BinaryOperator],
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          createdCallback,
          eventCallback,
          testId)),
        s"${prefix}disjunction")
  }
}
