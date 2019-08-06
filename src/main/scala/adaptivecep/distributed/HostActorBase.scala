package adaptivecep.distributed

import java.time.temporal.TemporalUnit
import java.time.{Clock, Instant}
import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.distributed.operator.{Host, NoHost, NodeHost}
import adaptivecep.simulation.ContinuousBoundedValue
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random
import rescala.default._
import rescala.{default, _}
import rescala.core.{CreationTicket, ReSerializable}
import rescala.default.{Evt, Signal, Var}

trait HostActorBase extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics]{
  //setup
  /**
    * A reference to Akka cluster system
    */
  val cluster: Cluster = Cluster(context.system)
  val interval = 3
  var optimizeFor: String = "latency"
  val clock: Clock = Clock.systemDefaultZone
  val random: Random = new Random(clock.millis())

  var measurementTask: Cancellable = _

  var node: Option[ActorRef] = Some(self)

  var latencies: Map[NodeHost, scala.concurrent.duration.Duration] = Map.empty[NodeHost, scala.concurrent.duration.Duration]

  /// Which actor is responsible for which node  NodeHost -> ActorRef
  /// NodeHost from CEP System
  /**
    * Maps between the actors ref created when the query is initialized and the hosts
    * they are supposed to be running on
    * used only for centralized placement
    */
  var hostToNodeMap: Map[NodeHost, ActorRef] = Map.empty[NodeHost, ActorRef]
  var throughputMeasureMap: Map[Host, Int] = Map.empty[Host, Int] withDefaultValue(0)
  var throughputStartMap: Map[Host, (Instant, Instant)] = Map.empty[Host, (Instant, Instant)] withDefaultValue((clock.instant(), clock.instant()))
  var costs: Map[Host, Cost] = Map.empty[Host, Cost].withDefaultValue(Cost(FiniteDuration(0, TimeUnit.SECONDS), 100))

  var hostMap: Map[ActorRef, NodeHost] = Map.empty[ActorRef, NodeHost]

  val hosts: Var[Set[NodeHost]] = Var(Set.empty[NodeHost])(ReSerializable.doNotSerialize, "consumers")
  val tick: Evt[Unit] = Evt[Unit]()

  var simulatedCosts: Map[NodeHost, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])] =
    Map.empty[NodeHost, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]


  var hostProps: HostPropsSimulator = HostPropsSimulator(simulatedCosts)

  case class HostPropsSimulator(costs : Map[NodeHost, (ContinuousBoundedValue[Duration], ContinuousBoundedValue[Double])]) {
    def advance = HostPropsSimulator(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth.advance)) })
    def advanceLatency = HostPropsSimulator(
      costs map { case (host, (latency, bandwidth)) => (host, (latency.advance, bandwidth)) })
    def advanceBandwidth = HostPropsSimulator(
      costs map { case (host, (latency, bandwidth)) => (host, (latency, bandwidth.advance)) })
  }

  def reportCostsToNode(): Unit = {
    var result = Map.empty[ActorRef, Cost].withDefaultValue(Cost(FiniteDuration(0, TimeUnit.SECONDS), 100))
    hostToNodeMap.foreach(host =>
      if(hostPropsToMap.contains(host._1)){
        result += host._2 -> hostPropsToMap(host._1)
      }
    )
    if(node.isDefined){
      node.get ! CostReport(result)
    }
  }

  object latency {
    implicit val addDuration: (Duration, Duration) => Duration = _ + _

    val template = ContinuousBoundedValue[Duration](
      Duration.Undefined,
      min = 2.millis, max = 100.millis,
      () => (2.millis - 4.milli * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 5.milli + 95.millis * random.nextDouble)
  }

  object bandwidth {
    implicit val addDouble: (Double, Double) => Double = _ + _

    val template = ContinuousBoundedValue[Double](
      0,
      min = 250, max = 5000,
      () => (100 - 200 * random.nextDouble, 1 + random.nextInt(10)))

    def apply() =
      template copy (value = 1000 + 4000 * random.nextDouble)
  }

  def hostPropsToMap: Map[Host, Cost] = {
    hostProps.costs map { case (host, (latency, bandwidth)) => (host, Cost(latency.value, bandwidth.value)) }
  }

  override def preStart(): Unit = {
    tick += {_ => measureCosts(hosts.now)}
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    startSimulation()
    startCostMeasurement()
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def measureCosts(hosts: Set[NodeHost]) = {
    val now = clock.instant()
    for (host <- hosts){
      if(hostPropsToMap.contains(host)) {
        host.actorRef ! StartThroughPutMeasurement(now)
        context.system.scheduler.scheduleOnce(
          FiniteDuration((bandwidth.template.max / hostPropsToMap(host).bandwidth).toLong * 100, TimeUnit.MILLISECONDS),
          () => {
            host.actorRef ! EndThroughPutMeasurement(now.plusMillis(100), hostPropsToMap(host).bandwidth.toInt)
          })
        if (hostPropsToMap.contains(host)) {
          context.system.scheduler.scheduleOnce(
            FiniteDuration(hostPropsToMap(host).duration.toMillis * 2, TimeUnit.MILLISECONDS),
            () => {
              host.actorRef ! LatencyRequest(now)
            })
        } else {
          host.actorRef ! LatencyRequest(now)
        }
      }
    }
  }

  def startSimulation(): Unit = {context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.MILLISECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      if(optimizeFor == "latency"){
        hostProps = hostProps.advanceLatency
      } else if (optimizeFor == "bandwidth") {
        hostProps = hostProps.advanceBandwidth
      } else {
        hostProps = hostProps.advance
      }
      reportCostsToNode()
    })
    tick.fire()
  }

  def startCostMeasurement(): Unit ={
    measurementTask = context.system.scheduler.schedule(
      initialDelay = FiniteDuration((random.nextDouble * 3000).toLong, TimeUnit.MILLISECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        tick.fire()
    })}

  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case Hosts(h)=>
      h.foreach(host => hostMap = hostMap + (host -> NodeHost(host)))
      hosts.set(hostMap.values.toSet)
      hosts.now.foreach(host => simulatedCosts += host -> (latency(), bandwidth()))
      hostProps = HostPropsSimulator(simulatedCosts)
    case Node(actorRef) =>{
      node = Some(actorRef)
    }
    case OptimizeFor(o) => optimizeFor = o
    case HostToNodeMap(m) =>
      hostToNodeMap = m
    case HostPropsRequest =>
      sender() ! HostPropsResponse(costs)
    case LatencyRequest(t)=>
      sender() ! LatencyResponse(t)
    case LatencyResponse(t) =>
      costs += hostMap(sender()) -> Cost(FiniteDuration(java.time.Duration.between(t, clock.instant()).dividedBy(2).toMillis, TimeUnit.MILLISECONDS), costs(hostMap(sender())).bandwidth)
    case StartThroughPutMeasurement(instant) =>
      if(hostMap.contains(sender())){
        throughputStartMap += hostMap(sender()) -> (instant, clock.instant())
        throughputMeasureMap += hostMap(sender()) -> 0
      }
    case TestEvent =>
      if(hostMap.contains(sender())){
        throughputMeasureMap += hostMap(sender()) -> (throughputMeasureMap(hostMap(sender())) + 1)
      }
    case EndThroughPutMeasurement(instant, actual) =>

      if(hostMap.contains(sender()) && throughputStartMap.contains(hostMap(sender()))){
        val senderDiff = java.time.Duration.between(throughputStartMap(hostMap(sender()))._1, instant)
        val receiverDiff = java.time.Duration.between(throughputStartMap(hostMap(sender()))._2, clock.instant())
        val calcBandwidth = (senderDiff.toMillis.toDouble / receiverDiff.toMillis.toDouble) * ((bandwidth.template.max / senderDiff.toMillis) * 100/*throughputMeasureMap(sender())*/)
        sender() ! ThroughPutResponse(calcBandwidth.toInt)
        throughputMeasureMap += hostMap(sender()) -> 0
      }
    case ThroughPutResponse(r) =>
      if(hostMap.contains(sender())){
        costs += hostMap(sender()) -> Cost(costs(hostMap(sender())).duration, hostPropsToMap(hostMap(sender())).bandwidth) //TODO: put in actual measurements (r)
      }
    case _ =>
  }

  def send(receiver: Host, message: Any): Unit ={
    receiver match {
      case host: NodeHost =>
        if(hostPropsToMap.contains(receiver)){
        context.system.scheduler.scheduleOnce(
          FiniteDuration(hostPropsToMap(receiver).duration.toMillis, TimeUnit.MILLISECONDS),
          () => {host.actorRef ! message})
      }
      else {
        host.actorRef ! message
      }
      case NoHost => println("[ERROR]: Tried sending message to non existent Host")
    }

  }
}
