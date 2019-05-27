package adaptivecep.graph.factory

import akka.actor.{ActorRef, ActorSystem, Props}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed
import adaptivecep.distributed.operator.{ActiveOperator, Operator}
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._

object GraphFactory {

  def createImpl(
                  actorSystem: ActorSystem,
                  query: Query,
                  publishers: Map[String, ActorRef],
                  frequencyMonitorFactory: MonitorFactory,
                  latencyMonitorFactory: MonitorFactory,
                  createdCallback: () => Any,
                  eventCallback: (Event) => Any): ActorRef = query match {
    case streamQuery: StreamQuery =>
      actorSystem.actorOf(Props(
        StreamNode(
          streamQuery.requirements,
          streamQuery.publisherName,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "stream")
    case sequenceQuery: SequenceQuery =>
      actorSystem.actorOf(Props(
        SequenceNode(
          sequenceQuery.requirements,
          sequenceQuery.s1.publisherName,
          sequenceQuery.s2.publisherName,
          getQueryLength(sequenceQuery.s1),
          getQueryLength(sequenceQuery.s2),
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "sequence")
    case filterQuery: FilterQuery =>
      actorSystem.actorOf(Props(
        FilterNode(
          filterQuery.requirements,
          filterQuery.cond,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "filter")
    case dropElemQuery: DropElemQuery =>
      actorSystem.actorOf(Props(
        DropElemNode(
          dropElemQuery.requirements,
          elemToBeDropped(dropElemQuery),
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "dropelem")
    case selfJoinQuery: SelfJoinQuery =>
      actorSystem.actorOf(Props(
        SelfJoinNode(
          selfJoinQuery.requirements,
          getWindowType(selfJoinQuery.w1),
          getWindowSize(selfJoinQuery.w1),
          getWindowType(selfJoinQuery.w2),
          getWindowSize(selfJoinQuery.w2),
          getQueryLength(selfJoinQuery),
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "selfjoin")
    case joinQuery: JoinQuery =>
      actorSystem.actorOf(Props(
        JoinNode(
          joinQuery.requirements,
          getWindowType(joinQuery.w1),
          getWindowSize(joinQuery.w1),
          getWindowType(joinQuery.w2),
          getWindowSize(joinQuery.w2),
          getQueryLength(joinQuery.sq1),
          getQueryLength(joinQuery.sq2),
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "join")
    case conjunctionQuery: ConjunctionQuery =>
      actorSystem.actorOf(Props(
        ConjunctionNode(
          conjunctionQuery.requirements,
          getQueryLength(conjunctionQuery.sq1),
          getQueryLength(conjunctionQuery.sq2),
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          Some(createdCallback),
          Some(eventCallback))),
        "conjunction")
    case disjunctionQuery: DisjunctionQuery =>
      actorSystem.actorOf(Props(
        DisjunctionNode(
          disjunctionQuery.requirements,
          getQueryLength(disjunctionQuery),
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
                 query: Query1[A],
                 publishers: Map[String, ActorRef],
                 frequencyMonitorFactory: MonitorFactory,
                 latencyMonitorFactory: MonitorFactory,
                 createdCallback: () => Any)(
                 eventCallback: (A) => Any): ActorRef =
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
                    query: Query2[A, B],
                    publishers: Map[String, ActorRef],
                    frequencyMonitorFactory: MonitorFactory,
                    latencyMonitorFactory: MonitorFactory,
                    createdCallback: () => Any)(
                    eventCallback: (A, B) => Any): ActorRef =
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
                       query: Query3[A, B, C],
                       publishers: Map[String, ActorRef],
                       frequencyMonitorFactory: MonitorFactory,
                       latencyMonitorFactory: MonitorFactory,
                       createdCallback: () => Any)(
                       eventCallback: (A, B, C) => Any): ActorRef =
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
                          query: Query4[A, B, C, D],
                          publishers: Map[String, ActorRef],
                          frequencyMonitorFactory: MonitorFactory,
                          latencyMonitorFactory: MonitorFactory,
                          createdCallback: () => Any)(
                          eventCallback: (A, B, C, D) => Any): ActorRef =
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
                             query: Query5[A, B, C, D, E],
                             publishers: Map[String, ActorRef],
                             frequencyMonitorFactory: MonitorFactory,
                             latencyMonitorFactory: MonitorFactory,
                             createdCallback: () => Any)(
                             eventCallback: (A, B, C, D, E) => Any): ActorRef =
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
                                query: Query6[A, B, C, D, E, F],
                                publishers: Map[String, ActorRef],
                                frequencyMonitorFactory: MonitorFactory,
                                latencyMonitorFactory: MonitorFactory,
                                createdCallback: () => Any)(
                                eventCallback: (A, B, C, D, E, F) => Any): ActorRef =
    createImpl(
      actorSystem,
      query.asInstanceOf[Query],
      publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      createdCallback,
      toFunEventAny(eventCallback))


  def initialize(actorSystem: ActorSystem,
                 query: Query,
                 publishers: Map[String, ActorRef],
                 frequencyMonitorFactory: MonitorFactory,
                 latencyMonitorFactory: MonitorFactory,
                 callback: Option[Event => Any]
                 ): ActorRef = {
    query match {
      case streamQuery: StreamQuery =>
        initializeStreamQuery(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  streamQuery, false)
      case sequenceQuery: SequenceQuery =>
        initializeSequenceNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  sequenceQuery, false)
      case filterQuery: FilterQuery =>
        initializeFilterNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  filterQuery, false)
      case dropElemQuery: DropElemQuery =>
        initializeDropElemNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  dropElemQuery, false)
      case selfJoinQuery: SelfJoinQuery =>
        initializeSelfJoinNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  selfJoinQuery, false)
      case joinQuery: JoinQuery =>
        initializeJoinNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  joinQuery, false)
      case conjunctionQuery: ConjunctionQuery =>
        initializeConjunctionNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  conjunctionQuery, false)
      case disjunctionQuery: DisjunctionQuery =>
        initializeDisjunctionNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, disjunctionQuery, false)
    }
  }

  private def initializeStreamQuery(actorSystem: ActorSystem,
                                    publishers: Map[String, ActorRef],
                                    frequencyMonitorFactory: MonitorFactory,
                                    latencyMonitorFactory: MonitorFactory,
                                    callback: Option[Event => Any],
                                    streamQuery: StreamQuery,
                                    consumer: Boolean) = {
    val ref = actorSystem.actorOf(Props(
      StreamNode(
        streamQuery.requirements,
        streamQuery.publisherName, publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    ref
  }

  private def initializeDisjunctionNode(actorSystem: ActorSystem,
                                        publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,

                                        callback: Option[Event => Any],
                                        disjunctionQuery: DisjunctionQuery,
                                        consumer: Boolean) = {
    val length = getQueryLength(disjunctionQuery)
    val ref = actorSystem.actorOf(Props(
      DisjunctionNode(
        disjunctionQuery.requirements,
        length,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    connectBinaryNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, disjunctionQuery.sq1, disjunctionQuery.sq2, ref, consumer)
    ref
  }

  private def initializeConjunctionNode(actorSystem: ActorSystem,
                                        publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        conjunctionQuery: ConjunctionQuery,
                                        consumer: Boolean) = {
    val length1 = getQueryLength(conjunctionQuery.sq1)
    val length2 = getQueryLength(conjunctionQuery.sq2)
    val props = actorSystem.actorOf(Props(
      ConjunctionNode(
        conjunctionQuery.requirements,
        length1,
        length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    connectBinaryNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, conjunctionQuery.sq1, conjunctionQuery.sq2, props, consumer)
    props
  }

  private def initializeJoinNode(actorSystem: ActorSystem,
                                 publishers: Map[String, ActorRef],
                                 frequencyMonitorFactory: MonitorFactory,
                                 latencyMonitorFactory: MonitorFactory,
                                 callback: Option[Event => Any],
                                 joinQuery: JoinQuery,
                                 consumer: Boolean) = {
    val wt1 = getWindowType(joinQuery.w1)
    val ws1 = getWindowSize(joinQuery.w1)
    val wt2 = getWindowType(joinQuery.w2)
    val ws2 = getWindowSize(joinQuery.w2)
    val length1 = getQueryLength(joinQuery.sq1)
    val length2 = getQueryLength(joinQuery.sq2)
    val props = actorSystem.actorOf(Props(
      JoinNode(
        joinQuery.requirements,
        wt1, ws1, wt2, ws2, length1, length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    connectBinaryNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, joinQuery.sq1, joinQuery.sq2, props, consumer)
    props
  }

  private def initializeSelfJoinNode(actorSystem: ActorSystem,
                                     publishers:              Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory:   MonitorFactory,
                                     callback:                Option[Event => Any],
                                     selfJoinQuery:           SelfJoinQuery,
                                     consumer:                Boolean) = {
    val wt1 = getWindowType(selfJoinQuery.w1)
    val ws1 = getWindowSize(selfJoinQuery.w1)
    val wt2 = getWindowType(selfJoinQuery.w2)
    val ws2 = getWindowSize(selfJoinQuery.w2)
    val length = getQueryLength(selfJoinQuery)
    val props = actorSystem.actorOf(Props(
      SelfJoinNode(
        selfJoinQuery.requirements,
        wt1, ws1, wt2, ws2, length,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    connectUnaryNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, selfJoinQuery.sq, props, consumer)
    props
  }

  private def initializeDropElemNode(actorSystem: ActorSystem,
                                     publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     dropElemQuery: DropElemQuery,
                                     consumer: Boolean) = {
    val drop = elemToBeDropped(dropElemQuery)
    val props = actorSystem.actorOf(Props(
      DropElemNode(
        dropElemQuery.requirements,
        drop,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    connectUnaryNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, dropElemQuery.sq, props, consumer)
    props
  }

  private def initializeFilterNode(actorSystem: ActorSystem,
                                   publishers: Map[String, ActorRef],
                                   frequencyMonitorFactory: MonitorFactory,
                                   latencyMonitorFactory: MonitorFactory,
                                   callback: Option[Event => Any],
                                   filterQuery: FilterQuery,
                                   consumer: Boolean) = {
    val cond = filterQuery.cond
    val props = actorSystem.actorOf(Props(
      FilterNode(
        filterQuery.requirements,
        cond.asInstanceOf[Event => Boolean],
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    connectUnaryNode(actorSystem, publishers, frequencyMonitorFactory, latencyMonitorFactory, filterQuery.sq, props, consumer)
    props
  }

  private def initializeSequenceNode(actorSystem: ActorSystem,
                                     publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     sequenceQuery: SequenceQuery,
                                     consumer: Boolean) = {
    val length1 = getQueryLength(sequenceQuery.s1)
    val length2 = getQueryLength(sequenceQuery.s2)
    val props = actorSystem.actorOf(Props(
      SequenceNode(
        sequenceQuery.requirements,
        sequenceQuery.s1.publisherName,
        sequenceQuery.s2.publisherName,
        length1,
        length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback)))
    props
  }

  private def connectUnaryNode(actorSystem: ActorSystem,
                               publishers: Map[String, ActorRef],
                               frequencyMonitorFactory: MonitorFactory,
                               latencyMonitorFactory: MonitorFactory,
                               query: Query,
                               props: ActorRef,
                               consumer: Boolean) : Unit = {
    val child = initialize(actorSystem, query, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      None)
      props ! Child1(child)
      child ! Parent(props)
  }

  private def connectBinaryNode(actorSystem: ActorSystem,
                                publishers: Map[String, ActorRef],
                                frequencyMonitorFactory: MonitorFactory,
                                latencyMonitorFactory: MonitorFactory,
                                query1: Query,
                                query2: Query,
                                props: ActorRef,
                                consumer: Boolean) : Unit = {
    val child1 = initialize(actorSystem, query1, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      None)
    val child2 = initialize(actorSystem, query2, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      None)

      props ! Child2(child1, child2)
      child1 ! Parent(props)
      child2 ! Parent(props)

  }

  def getQueryLength(query: Query): Int = query match {
    case _: Query1[_] => 1
    case _: Query2[_, _] => 2
    case _: Query3[_, _, _] => 3
    case _: Query4[_, _, _, _] => 4
    case _: Query5[_, _, _, _, _] => 5
    case _: Query6[_, _, _, _, _, _] => 6
  }

  def getQueryLength(noReqStream: NStream): Int = noReqStream match {
    case _: NStream1[_] => 1
    case _: NStream2[_, _] => 2
    case _: NStream3[_, _, _] => 3
    case _: NStream4[_, _, _, _] => 4
    case _: NStream5[_, _, _, _, _] => 5
  }

  def elemToBeDropped(query: Query): Int = query match {
    case DropElem1Of2(_, _) => 1
    case DropElem1Of3(_, _) => 1
    case DropElem1Of4(_, _) => 1
    case DropElem1Of5(_, _) => 1
    case DropElem1Of6(_, _) => 1
    case DropElem2Of2(_, _) => 2
    case DropElem2Of3(_, _) => 2
    case DropElem2Of4(_, _) => 2
    case DropElem2Of5(_, _) => 2
    case DropElem2Of6(_, _) => 2
    case DropElem3Of3(_, _) => 3
    case DropElem3Of4(_, _) => 3
    case DropElem3Of5(_, _) => 3
    case DropElem3Of6(_, _) => 3
    case DropElem4Of4(_, _) => 4
    case DropElem4Of5(_, _) => 4
    case DropElem4Of6(_, _) => 4
    case DropElem5Of5(_, _) => 5
    case DropElem5Of6(_, _) => 5
    case DropElem6Of6(_, _) => 6
    case _ => 1
  }

  def getWindowType(window: Window): String = window match {
    case _: SlidingInstances => "SI"
    case _: TumblingInstances => "TI"
    case _: SlidingTime => "ST"
    case _: TumblingTime => "TT"
  }

  def getWindowSize(window: Window): Int = window match {
    case SlidingInstances(i) => i
    case TumblingInstances(i) => i
    case SlidingTime(i) => i
    case TumblingTime(i) => i
  }
}