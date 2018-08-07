package adaptivecep.graph.factory

import akka.actor.{ActorRef, ActorSystem, Props}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._
import shapeless.ops.hlist.HKernelAux
import shapeless.{::, HList, HNil}

object GraphFactory {

  def createImpl[A <: HList, B <: HList](
      actorSystem: ActorSystem,
      query: Query,
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any,
      eventCallback: (Event) => Any)(implicit opA: HKernelAux[A], opB: HKernelAux[B]): ActorRef = query match {
    case streamQuery: StreamQuery =>
      actorSystem.actorOf(Props(
        StreamNode(
          streamQuery, publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "stream")
    case sequenceQuery: SequenceQuery[A, B] =>
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
    case conjunctionQuery: ConjunctionQuery =>
      actorSystem.actorOf(Props(
        ConjunctionNode(
          conjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))(opA)),
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
  }

  // This is why `eventCallback` is listed separately:
  // https://stackoverflow.com/questions/21147001/why-scala-doesnt-infer-type-from-generic-type-parameters
  def create[A](
      actorSystem: ActorSystem,
      query: HListQuery[A :: HNil],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A) => Any)(implicit op: HKernelAux[A :: HNil]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B](
      actorSystem: ActorSystem,
      query: HListQuery[A :: B :: HNil],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B) => Any)(implicit op: HKernelAux[A :: B :: HNil]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C](
      actorSystem: ActorSystem,
      query: HListQuery[A :: B :: C :: HNil],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C) => Any)(implicit op: HKernelAux[A :: B :: C :: HNil]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D](
      actorSystem: ActorSystem,
      query: HListQuery[A :: B :: C :: D :: HNil],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C, D) => Any)(implicit op: HKernelAux[A :: B :: C :: D :: HNil]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D, E](
      actorSystem: ActorSystem,
      query: HListQuery[A :: B :: C :: D :: E :: HNil],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C, D, E) => Any)(implicit op: HKernelAux[A :: B :: C :: D :: E :: HNil]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

  def create[A, B, C, D, E, F](
      actorSystem: ActorSystem,
      query: HListQuery[A :: B :: C :: D :: E :: F :: HNil],
      publishers: Map[String, ActorRef],
      frequencyMonitorFactory: MonitorFactory,
      latencyMonitorFactory: MonitorFactory,
      createdCallback: () => Any)(
      eventCallback: (A, B, C, D, E, F) => Any)(implicit op: HKernelAux[A :: B :: C :: D :: E :: F :: HNil]): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))

}
