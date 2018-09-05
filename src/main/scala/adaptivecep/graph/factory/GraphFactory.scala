package adaptivecep.graph.factory

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, ActorSystem, Props}
import util.tuplehlistsupport.{Length, FromTraversable}

object GraphFactory {

  def createImpl(
      actorSystem: ActorSystem,
      query: IQuery,
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any,
      eventCallback: (Event) => Any): ActorRef = query match {
    case streamQuery: StreamQuery =>
      actorSystem.actorOf(Props(
        StreamNode(
          streamQuery, publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "stream")
    case sequenceQuery: SequenceQuery[_, _] =>
      actorSystem.actorOf(Props(
        SequenceNode(
          sequenceQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "sequence")
    case filterQuery: FilterQuery =>
      actorSystem.actorOf(Props(
        FilterNode(
          filterQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "filter")
    case dropElemQuery: DropElemQuery =>
      actorSystem.actorOf(Props(
        DropElemNode(
          dropElemQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "dropelem")
    case selfJoinQuery: SelfJoinQuery =>
      actorSystem.actorOf(Props(
        SelfJoinNode(
          selfJoinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "selfjoin")
    case joinQuery: JoinQuery =>
      actorSystem.actorOf(Props(
        JoinNode(
          joinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "join")
    case joinOnQuery: JoinOnQuery =>
      actorSystem.actorOf(Props(
        JoinOnNode(
          joinOnQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "joinOn")
    case conjunctionQuery: ConjunctionQuery =>
      actorSystem.actorOf(Props(
        ConjunctionNode(
          conjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "conjunction")
    case disjunctionQuery: DisjunctionQuery =>
      actorSystem.actorOf(Props(
        DisjunctionNode(
          disjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "disjunction")
    // only to avoid warning that match is not exhaustive
    case _: Query[_] => throw new IllegalArgumentException("Query should not be passed as an argument")
  }

  // This is why `eventCallback` is listed separately:
  // https://stackoverflow.com/questions/21147001/why-scala-doesnt-infer-type-from-generic-type-parameters
  def create[T](
      actorSystem: ActorSystem,
      query: Query[T],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (T) => Any)(implicit length: Length[T], fl: FromTraversable[T]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[IQuery],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny[T](eventCallback))
}
