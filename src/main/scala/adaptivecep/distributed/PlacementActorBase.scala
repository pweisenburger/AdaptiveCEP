package adaptivecep.distributed

import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.data.Events.Event
import adaptivecep.data.Queries.{Operator => _, _}
import adaptivecep.distributed
import adaptivecep.distributed.operator.Operator
import adaptivecep.distributed.operator._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos.MonitorFactory
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Deploy, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.RemoteScope
import rescala.default._
import rescala.{default, _}
import rescala.core.{CreationTicket, ReSerializable}
import rescala.default.{Evt, Signal, Var}
import helper._
import rescala.parrp.ParRP

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.Random

trait PlacementActorBase extends Actor with ActorLogging with System{

  val actorSystem: ActorSystem
  val query: Query
  val publishers: Map[String, ActorRef]
  val publisherHosts: Map[String, Host]
  val frequencyMonitorFactory: MonitorFactory
  val latencyMonitorFactory: MonitorFactory
  val bandwidthMonitorFactory: MonitorFactory
  val here: NodeHost
  val testHosts: Set[ActorRef]
  val optimizeFor: String

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing

  val cluster: Cluster = Cluster(context.system)
  val r: Random = scala.util.Random

  var propsOperators: Map[Props, Operator] = Map.empty[Props, Operator]
  var propsActors: Map[Props, ActorRef] = Map.empty[Props, ActorRef]
  var parents: Map[Operator, Option[Operator]] = Map.empty[Operator, Option[Operator]] withDefaultValue None

  //var hostProps: Map[Host, HostProps] = Map.empty[Host, HostProps].withDefaultValue(HostProps(Seq.empty, Seq.empty))
  var consumers1: Seq[Operator] = Seq.empty[Operator]
  var costsMap: Map[Host, Map[Host, Cost]] = Map.empty[Host, Map[Host, Cost]].withDefaultValue(Map.empty[Host, Cost].withDefaultValue(Cost(FiniteDuration(0, TimeUnit.SECONDS), 100)))
  var hostMap: Map[ActorRef, NodeHost] = Map(here.actorRef -> here)
  var hostToNodeMap: Map[NodeHost, ActorRef] = Map.empty[NodeHost, ActorRef]

  val interval = 500

  var justAdapted = false
  var firstTimePlacement = true

  val costSignal: Var[Map[Host, Map[Host, Cost]]] = Var(costsMap)(ReSerializable.doNotSerialize, "cost")
  val hosts: Var[Set[NodeHost]] = Var(Set.empty[NodeHost])(ReSerializable.doNotSerialize, "hosts")
  val qos: Signal[Map[Host, HostProps]] = Signal{hostProps(costSignal(), hosts().map(h => h.asInstanceOf[Host]))}
  val consumers: Var[Seq[Operator]] = Var(Seq.empty[Operator])(ReSerializable.doNotSerialize, "consumers")
  val producers: Var[Set[Operator]] = Var(Set.empty[Operator])(ReSerializable.doNotSerialize, "producers")
  val operators: Var[Set[Operator]] = Var(Set.empty[Operator])(ReSerializable.doNotSerialize, "operators")
  val placement: Var[Map[Operator, Host]] = Var(Map.empty[Operator, Host] withDefaultValue NoHost)(ReSerializable.doNotSerialize, "placement")
  val demandViolated: default.Evt[Set[Requirement]] = Evt[Set[Requirement]]()




  //val createdCallback: Option[() => Any] = () => println("STATUS:\t\tGraph has been created.")
  val eventCallback: Event => Any = {
    // Callback for `query1`:
    //case Event3(Left(i1), Left(i2), Left(f)) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
    //case Event3(Right(s), _, _)              => println(s"COMPLEX EVENT:\tEvent1($s)")
    // Callback for `query2`:
    //case Event4(i1, i2, f, s)             => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f,$s)")
    // This is necessary to avoid warnings about non-exhaustive `match`:
    case _                             => //println("what the hell")
  }

  def placeAll(map: Map[Operator, Host]): Unit

  def place(operator: Operator, host: Host): Unit


  val adaption: reactives.Event[Map[Operator, Host], ParRP] = demandViolated map { _ =>
    adapt(qos(), consumers(), placement()): Map[Operator, Host]
  }

  override def preStart(): Unit = {
    adaption observe placeAll
    println(optimizeFor)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])

    var latencyStub: Seq[(Host, Duration)] = Seq.empty[(Host, Duration)]
    var bandwidthStub: Seq[(Host, Double)] = Seq.empty[(Host, Double)]
    testHosts.foreach(host => {
      val nodeHost = NodeHost(host)
      hostMap += host -> nodeHost
      latencyStub = latencyStub :+ (nodeHost, Duration.Inf)
      bandwidthStub = bandwidthStub :+ (nodeHost, 0.0)
      hosts.set(hosts.now + nodeHost)
    })
  }

  override def postStop(): Unit ={
    propsActors.keys.foreach(key => propsActors(key) ! Kill)
    cluster.unsubscribe(self)
  }

  def adapt(qos: Map[Host, HostProps], consumers: Seq[Operator], placement: Map[Operator, Host]): Map[Operator, Host] = {
    optimizeFor match {
      case "latency" => return placeOptimizingLatency(qos, consumers, placement)
      case "bandwidth" => return placeOptimizingBandwidth(qos, consumers, placement)
      case "latencybandwidth" => return placeOptimizingLatencyAndBandwidth(qos, consumers, placement)
      case _ => println("ERROR: Typo in optimizeFor Parameter", optimizeFor)
    }
    Map.empty[Operator, Host]
  }

  override def receive: Receive = {
    case InitializeQuery =>
      context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
        runnable = () => {
          hosts.now.foreach{
            host => host.asInstanceOf[NodeHost].actorRef ! HostPropsRequest
          }
        })
      initialize(query, publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, Some(eventCallback), consumer = true)
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case MemberExited(member) =>
      log.info("Member exiting: {}", member)
    case RequirementsNotMet(requirements) =>
      demandViolated.fire(requirements)
    case Start =>
      println("PLACEMENT ACTOR: starting")
      demandViolated.fire(null)
    case HostPropsResponse(costMap) =>
      costsMap += hostMap(sender()) -> costMap.map(h => h._1 -> h._2)
      costSignal.set(costsMap)
    case _ =>
  }

  private def latencySelector(props: HostProps, host: Host): Duration = {
    if(host.equals(NoHost)){
      return Duration.apply(50, TimeUnit.DAYS)
    }
    val latency = props.latency collectFirst { case (`host`, latency) => latency }
    if(latency.isDefined){
      latency.get
    }
    else Duration(50, TimeUnit.DAYS)

  }

  private def bandwidthSelector(props: HostProps, host: Host): Double = {
    if(host.equals(NoHost)){
      return 0.0
    }
    val bandwidth = props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }
    if(bandwidth.isDefined){
      bandwidth.get
    }
    else 0.0

  }
  private def latencyBandwidthSelector(props: HostProps, host: Host): (Duration, Double) = {
    if(host.equals(NoHost)){
      return (Duration.apply(50, TimeUnit.DAYS), 0.0)
    }
    val latency = props.latency collectFirst { case (`host`, latency) => latency }
    val bandwidth = props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }
    if(latency.isDefined && bandwidth.isDefined){
      (latency.get, bandwidth.get)
    }
    else (Duration.apply(50, TimeUnit.DAYS), 0.0)
  }

  private def avg(durations: Seq[Duration]): Duration =
    if (durations.isEmpty)
      Duration.Zero
    else
      durations.foldLeft[Duration](Duration.Zero) { _ + _ } / durations.size

  private def avg(numerics: Seq[Double]): Double =
    if (numerics.isEmpty)
      0.0
    else
      numerics.sum / numerics.size


  private def measure[T: Ordering](
                                    selector: (HostProps, Host) => T,
                                    optimizing: Optimizing,
                                    zero: T,
                                    qos: Map[Host, HostProps],
                                    consumers: Seq[Operator],
                                    placement: Map[Operator, Host])(
                                    merge: (T, T) => T)(
                                    avg: Seq[T] => T)(
                                    host: Operator => Host): T = {
    def measure(operator: Operator): T =
      if (operator.dependencies.isEmpty)
        zero
      else
        minmax(optimizing, operator.dependencies map { dependentOperator =>
          merge(measure(dependentOperator), selector(qos.apply(host(operator)), host(dependentOperator)))
        })

    avg(consumers map measure)
  }

  def placeOptimizingLatency(qos: Map[Host, HostProps], consumers: Seq[Operator], placement: Map[Operator, Host]): Map[Operator, Host] = {
    val measureLatency = measure(latencySelector, Minimizing, Duration.Zero, qos, consumers, placement) { _ + _ } { avg } _

    val placementsA = placeOptimizingHeuristicA(latencySelector, Minimizing, qos, consumers, placement)
    val durationA = measureLatency { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencySelector, Minimizing, qos, consumers, placement) { _ + _ }
    val durationB = measureLatency { placementsB(_) }

    //placeAll((if (durationA < durationB) placementsA else placementsB).toMap)
    (if (durationA < durationB) placementsA else placementsB).toMap

  }

  def placeOptimizingBandwidth(qos: Map[Host, HostProps], consumers: Seq[Operator], placement: Map[Operator, Host]): Map[Operator, Host] = {
    val measureBandwidth = measure(bandwidthSelector, Maximizing, Double.MaxValue, qos, consumers, placement) { math.min } { avg } _

    val placementsA = placeOptimizingHeuristicA(bandwidthSelector, Maximizing, qos, consumers, placement)
    val bandwidthA = measureBandwidth { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(bandwidthSelector, Maximizing, qos, consumers, placement) { math.min }
    val bandwidthB = measureBandwidth { placementsB(_) }

    //placeAll((if (bandwidthA > bandwidthB) placementsA else placementsB).toMap)
    (if (bandwidthA > bandwidthB) placementsA else placementsB).toMap
  }

  def placeOptimizingLatencyAndBandwidth(qos: Map[Host, HostProps], consumers: Seq[Operator], placement: Map[Operator, Host]): Map[Operator, Host] = {
    def average(durationNumerics: Seq[(Duration, Double)]): (Duration, Double) =
      durationNumerics.unzip match { case (latencies, bandwidths) => (avg(latencies), avg(bandwidths)) }

    def merge(durationNumeric0: (Duration, Double), durationNumeric1: (Duration, Double)): (Duration, Double) =
      (durationNumeric0, durationNumeric1) match { case ((duration0, numeric0), (duration1, numeric1)) =>
        (duration0 + duration1, math.min(numeric0, numeric1))
      }

    implicit val ordering = new Ordering[(Duration, Double)] {
      def abs(x: Duration) = if (x < Duration.Zero) -x else x
      def compare(x: (Duration, Double), y: (Duration, Double)) = ((-x._1, x._2), (-y._1, y._2)) match {
        case ((d0, n0), (d1, n1)) if d0 == d1 && n0 == n1 => 0
        case ((d0, n0), (d1, n1)) if d0 < d1 && n0 < n1 => -1
        case ((d0, n0), (d1, n1)) if d0 > d1 && n0 > n1 => 1
        case ((d0, n0), (d1, n1)) =>
          math.signum((d0 - d1) / abs(d0 + d1) + (n0 - n1) / math.abs(n0 + n1)).toInt
      }
    }

    val measureBandwidth = measure(latencyBandwidthSelector, Maximizing, (Duration.Zero, Double.MaxValue) , qos, consumers, placement) { merge } { average } _

    val placementsA = placeOptimizingHeuristicA(latencyBandwidthSelector, Maximizing, qos, consumers, placement)
    val bandwidthA = measureBandwidth { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencyBandwidthSelector, Maximizing, qos, consumers, placement) { merge }
    val bandwidthB = measureBandwidth { placementsB(_) }

    (if (bandwidthA > bandwidthB) placementsA else placementsB).toMap
    //placeAll((if (bandwidthA > bandwidthB) placementsA else placementsB).toMap)
  }

  private def placeOptimizingHeuristicA[T: Ordering](selector: (HostProps, Host) => T,
                                                     optimizing: Optimizing,
                                                     qos: Map[Host, HostProps],
                                                     consumers: Seq[Operator],
                                                     placement: Map[Operator, Host]): collection.Map[Operator, Host] = {
    val placements = mutable.Map.empty[Operator, Host]

    def placeProducersConsumers(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeProducersConsumers(_, consumer = false) }
      if (consumer || operator.dependencies.isEmpty)
        placements += operator -> placement.apply(operator)
    }

    def placeIntermediates(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeIntermediates(_, consumer = false) }

      val host =
        if (!consumer && operator.dependencies.nonEmpty) {
          val valuesForHosts =
            qos.toSeq collect { case (host, props) if !(placements.values exists { _== host }) =>
              val propValues =
                operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }

              minmax(optimizing, propValues) -> host
            }

          if (valuesForHosts.isEmpty)
            throw new UnsupportedOperationException("not enough hosts")

          val (_, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }
          host
        }
        else
          placement.apply(operator)

      placements += operator -> host
    }

    consumers foreach { placeProducersConsumers(_, consumer = true) }
    consumers foreach { placeIntermediates(_, consumer = true) }
    placements
  }

  private def placeOptimizingHeuristicB[T: Ordering](selector: (HostProps, Host) => T,
                                                     optimizing: Optimizing,
                                                     qos: Map[Host, HostProps],
                                                     consumers: Seq[Operator],
                                                     placement: Map[Operator, Host])(
                                                      merge: (T, T) => T): collection.Map[Operator, Host] = {
    val previousPlacements = mutable.Map.empty[Operator, mutable.Set[Host]]
    val placements = mutable.Map.empty[Operator, Host]

    def allOperators(operator: Operator, parent: Option[Operator]): Seq[(Operator, Option[Operator])] =
      (operator -> parent) +: (operator.dependencies flatMap { allOperators(_, Some(operator)) })

    val operators = consumers. flatMap { allOperators(_, None) }
    operators foreach { case (operator, _) =>
      placements += operator -> placement.apply(operator)//operator.host
      previousPlacements += operator -> mutable.Set(placement.apply(operator))
    }

    @tailrec def placeOperators(): Unit = {
      val changed = operators map {
        case (operator, Some(parent)) if operator.dependencies.nonEmpty =>
          val valuesForHosts =
            qos.toSeq collect { case (host, props) if !(placements.values exists { _ == host }) && !(previousPlacements(operator) contains host) =>
              merge(
                minmax(optimizing, operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }),
                selector(qos.apply(placements(parent)), host)) -> host
            }
          val currentValue =
            merge(
              minmax(optimizing, operator.dependencies map { dependency =>
                selector(qos.apply(placements(operator)), placements(dependency))
              }),
              selector(qos.apply(placements(parent)), placements(operator)))
          val noPotentialPlacements =
            if (valuesForHosts.isEmpty) {
              if ((qos.keySet -- placements.values --previousPlacements(operator)).isEmpty)
                true
              else
                throw new UnsupportedOperationException("not enough hosts")
            }
            else
              false

          if (!noPotentialPlacements) {
            val (value, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }
            var changePlacement = false
            if(optimizeFor == "latency") {
              changePlacement = value < currentValue
            }
            else{
              changePlacement = value > currentValue
            }

            if (changePlacement) {
              placements += operator -> host
              previousPlacements(operator) += host
            }

            changePlacement
          }
          else
            false

        case _ =>
          false
      }

      if (changed contains true)
        placeOperators()
    }
    placeOperators()

    //println("PLACEMENT ACTOR Heuristic B", placements)
    placements
  }

  private def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]): T = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  private def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U): T = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }

  def initialize(query: Query,
                 publishers: Map[String, ActorRef],
                 frequencyMonitorFactory: MonitorFactory,
                 latencyMonitorFactory: MonitorFactory,
                 bandwidthMonitorFactory: MonitorFactory,
                 callback: Option[Event => Any],
                 consumer: Boolean ): Props = {
    query match {
      case streamQuery: StreamQuery =>
        initializeStreamQuery(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  streamQuery, consumer)
      case sequenceQuery: SequenceQuery =>
        initializeSequenceNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  sequenceQuery, consumer)
      case filterQuery: FilterQuery =>
        initializeFilterNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  filterQuery, consumer)
      case dropElemQuery: DropElemQuery =>
        initializeDropElemNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  dropElemQuery, consumer)
      case selfJoinQuery: SelfJoinQuery =>
        initializeSelfJoinNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  selfJoinQuery, consumer)
      case joinQuery: JoinQuery =>
        initializeJoinNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  joinQuery, consumer)
      case conjunctionQuery: ConjunctionQuery =>
        initializeConjunctionNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback,  conjunctionQuery, consumer)
      case disjunctionQuery: DisjunctionQuery =>
        initializeDisjunctionNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, callback, disjunctionQuery, consumer)
    }
  }

  private def initializeStreamQuery(publishers: Map[String, ActorRef],
                                    frequencyMonitorFactory: MonitorFactory,
                                    latencyMonitorFactory: MonitorFactory,
                                    bandwidthMonitorFactory: MonitorFactory,
                                    callback: Option[Event => Any],
                                    streamQuery: StreamQuery,
                                    consumer: Boolean) = {
    val props = Props(
      StreamNode(
        streamQuery.requirements,
        streamQuery.publisherName, publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    val operator = ActiveOperator(props, Seq.empty[Operator])
    placement.set(placement.now + (operator -> publisherHosts(streamQuery.publisherName)))
    producers.set(producers.now.+(operator))
    operators.set(operators.now.+(operator))
    propsOperators += props -> operator
    props
  }

  private def initializeDisjunctionNode(publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        bandwidthMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        disjunctionQuery: DisjunctionQuery,
                                        consumer: Boolean) = {
    val length = getQueryLength(disjunctionQuery)
    val props = Props(
      DisjunctionNode(
        disjunctionQuery.requirements,
        length,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,

        None,
        callback))
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, disjunctionQuery.sq1, disjunctionQuery.sq2, props, consumer)
    props
  }

  private def initializeConjunctionNode(publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        bandwidthMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        conjunctionQuery: ConjunctionQuery,
                                        consumer: Boolean) = {
    val length1 = getQueryLength(conjunctionQuery.sq1)
    val length2 = getQueryLength(conjunctionQuery.sq2)
    val props = Props(
      ConjunctionNode(
        conjunctionQuery.requirements,
        length1,
        length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,

        None,
        callback))
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, conjunctionQuery.sq1, conjunctionQuery.sq2, props, consumer)
    props
  }

  private def initializeJoinNode(publishers: Map[String, ActorRef],
                                 frequencyMonitorFactory: MonitorFactory,
                                 latencyMonitorFactory: MonitorFactory,
                                 bandwidthMonitorFactory: MonitorFactory,
                                 callback: Option[Event => Any],
                                 joinQuery: JoinQuery,
                                 consumer: Boolean) = {
    val wt1 = getWindowType(joinQuery.w1)
    val ws1 = getWindowSize(joinQuery.w1)
    val wt2 = getWindowType(joinQuery.w2)
    val ws2 = getWindowSize(joinQuery.w2)
    val length1 = getQueryLength(joinQuery.sq1)
    val length2 = getQueryLength(joinQuery.sq2)
    val props = Props(
      JoinNode(
        joinQuery.requirements,
        wt1, ws1, wt2, ws2, length1, length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,

        None,
        callback))
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, joinQuery.sq1, joinQuery.sq2, props, consumer)
    props
  }

  private def initializeSelfJoinNode(publishers:              Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory:   MonitorFactory,
                                     bandwidthMonitorFactory: MonitorFactory,
                                     callback:                Option[Event => Any],
                                     selfJoinQuery:           SelfJoinQuery,
                                     consumer:                Boolean) = {
    val wt1 = getWindowType(selfJoinQuery.w1)
    val ws1 = getWindowSize(selfJoinQuery.w1)
    val wt2 = getWindowType(selfJoinQuery.w2)
    val ws2 = getWindowSize(selfJoinQuery.w2)
    val length = getQueryLength(selfJoinQuery)
    val props = Props(
      SelfJoinNode(
        selfJoinQuery.requirements,
        wt1, ws1, wt2, ws2, length,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, selfJoinQuery.sq, props, consumer)
    props
  }

  private def initializeDropElemNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     bandwidthMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     dropElemQuery: DropElemQuery,
                                     consumer: Boolean) = {
    val drop = elemToBeDropped(dropElemQuery)
    val props = Props(
      DropElemNode(
        dropElemQuery.requirements,
        drop,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, dropElemQuery.sq, props, consumer)
    props
  }

  private def initializeFilterNode(publishers: Map[String, ActorRef],
                                   frequencyMonitorFactory: MonitorFactory,
                                   latencyMonitorFactory: MonitorFactory,
                                   bandwidthMonitorFactory: MonitorFactory,
                                   callback: Option[Event => Any],
                                   filterQuery: FilterQuery,
                                   consumer: Boolean) = {
    val cond = filterQuery.cond
    val props = Props(
      FilterNode(
        filterQuery.requirements,
        cond.asInstanceOf[Event => Boolean],
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, bandwidthMonitorFactory, filterQuery.sq, props, consumer)
    props
  }

  private def initializeSequenceNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     bandwidthMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     sequenceQuery: SequenceQuery,
                                     consumer: Boolean) = {
    val length1 = getQueryLength(sequenceQuery.s1)
    val length2 = getQueryLength(sequenceQuery.s2)
    val props = Props(
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
        callback))
    val operator = ActiveOperator(props, Seq.empty[Operator])
    placement.set(placement.now + (operator -> publisherHosts(sequenceQuery.s1.publisherName)))
    producers.set(producers.now.+(operator))
    operators.set(operators.now.+(operator))
    propsOperators += props -> operator
    props
  }

  private def connectUnaryNode(publishers: Map[String, ActorRef],
                               frequencyMonitorFactory: MonitorFactory,
                               latencyMonitorFactory: MonitorFactory,
                               bandwidthMonitorFactory: MonitorFactory,
                               query: Query,
                               props: Props,
                               consumer: Boolean) : Unit = {
    val child = initialize(query, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      bandwidthMonitorFactory, None, consumer = false)
    val childOperator = propsOperators(child)
    var operator = distributed.operator.ActiveOperator(props, Seq(childOperator))
    if (consumer) {
      //consumers = consumers :+ operator
      consumers.set(consumers.now :+ operator)
      placement.set(placement.now + (operator -> here))
    } else {
      operator = ActiveOperator(props, Seq(childOperator))
    }
    operators.set(operators.now.+(operator))
    propsOperators += props -> operator
    parents += childOperator -> Some(propsOperators(props))
  }

  private def connectBinaryNode(publishers: Map[String, ActorRef],
                                frequencyMonitorFactory: MonitorFactory,
                                latencyMonitorFactory: MonitorFactory,
                                bandwidthMonitorFactory: MonitorFactory,
                                query1: Query,
                                query2: Query,
                                props: Props,
                                consumer: Boolean) : Unit = {
    val child1 = initialize(query1, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      bandwidthMonitorFactory, None, consumer = false)
    val child2 = initialize(query2, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory,
      bandwidthMonitorFactory, None, consumer = false)
    val child1Operator = propsOperators(child1)
    val child2Operator = propsOperators(child2)
    var operator: ActiveOperator = null
    if (consumer) {
      operator = distributed.operator.ActiveOperator(props, Seq(child1Operator, child2Operator))
      //consumers = consumers :+ operator
      consumers.set(consumers.now :+ operator)
      placement.set(placement.now + (operator -> here))
    } else {
      operator = ActiveOperator(props, Seq(child1Operator, child2Operator))
    }
    operators.set(operators.now.+(operator))
    propsOperators += props -> operator
    parents += child1Operator -> Some(propsOperators(props))
    parents += child2Operator -> Some(propsOperators(props))
  }

  def getQueryLength(query: Query): Int = query match {
    case _: Query1[_] => 1
    case _: Query2[_, _] => 2
    case _: Query3[_, _, _] => 3
    case _: Query4[_, _, _, _] => 4
    case _: Query5[_, _, _, _, _] => 5
    case _: Query6[_, _, _, _, _, _] => 6
  }

  def getQueryLength(noReqStream: NStream):Int = noReqStream match {
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

  def getWindowType(window: Window): String = window match{
    case _ : SlidingInstances => "SI"
    case _ : TumblingInstances => "TI"
    case _ : SlidingTime => "ST"
    case _ : TumblingTime => "TT"
  }

  def getWindowSize(window: Window): Int = window match{
    case SlidingInstances(i) => i
    case TumblingInstances(i) => i
    case SlidingTime(i) => i
    case TumblingTime(i) => i
  }

}