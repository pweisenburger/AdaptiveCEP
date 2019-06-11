package adaptivecep.distributed

import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events._
import adaptivecep.data.Queries.Requirement
import adaptivecep.distributed.Stage.Stage
import adaptivecep.distributed.operator._
import adaptivecep.distributed.operator.{ActiveOperator, Host, HostProps, NodeHost, Operator, TentativeOperator, helper}
import akka.actor.{ActorRef, ActorSystem, Cancellable, Deploy, Props}
import akka.cluster.ClusterEvent._
import akka.remote
import rescala.default._
import rescala.{default, _}
import rescala.core.{CreationTicket, ReSerializable}
import rescala.default.{Evt, Signal, Var}
import rescala.parrp.ParRP
import rescala.reactives.Event

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


trait HostActorDecentralizedBase extends HostActorBase with System{

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing

  /**
    * Constant Values throughout the run
    */
  val degree: Int = 2
  val system: ActorSystem = context.system
  var consumer: Boolean = false

  /**
    * Hidden by implementation. No Need for reactives.
    */
  var activeOperator: Option[ActiveOperator] = None
  var tentativeOperator: Option[TentativeOperator] = None

  var tentativeHosts: Set[NodeHost] = Set.empty[NodeHost]

  var childrenCompletedChoosingTentatives: Set[ActorRef] = Set.empty[ActorRef]
  var childrenMigrated: Set[ActorRef] = Set.empty[ActorRef]
  var processedCostMessages: Set[ActorRef] = Set.empty[ActorRef]

  var operatorResponses: Set[ActorRef] = Set.empty[ActorRef]
  var latencyResponses: Set[ActorRef] = Set.empty[ActorRef]
  var bandwidthResponses: Set[ActorRef] = Set.empty[ActorRef]


  var childNode1: Option[ActorRef] = None //these 2 need to remain actoref since they are not hostactors
  var childNode2: Option[ActorRef] = None

  val thisHost: NodeHost = NodeHost(self)

  var parent: Option[NodeHost] = None

  var parentNode: Option[ActorRef] = None
  var parentHosts: Set[NodeHost] = Set.empty[NodeHost]


  /**
    * Required to Calculate new Placement
    */
  val stage: Var[Stage] = Var(Stage.TentativeOperatorSelection)(ReSerializable.doNotSerialize, "cost")
  //val optimumHosts: Var[Seq[NodeHost]] = Var(Seq.empty[NodeHost])(ReSerializable.doNotSerialize, "cost")

  val children: Var[Map[NodeHost, Set[NodeHost]]] = Var(Map.empty[NodeHost, Set[NodeHost]])(ReSerializable.doNotSerialize, "cost")
  val numberOfChildren: Signal[Int] = Signal{children().keys.size + children().values.foldLeft(0){(x,y) => x + y.size}}
  val qosInternal: Var[Map[NodeHost, Cost]] = Var(Map.empty[NodeHost, Cost])(ReSerializable.doNotSerialize, "cost")
  val qos: Signal[Map[Host, HostProps]] = Signal{Map.empty[Host, HostProps]}

  val placement: Var[Map[Operator, Host]] = Var(Map.empty[Operator, Host])(ReSerializable.doNotSerialize, "cost")
  val reversePlacement: Signal[Map[Host, Operator]] = Signal{placement().map(_.swap)}

  val operators: Signal[Set[Operator]] = Var(Set.empty[Operator])//is not being filled correctly as of now

  val demandViolated: default.Evt[Set[Requirement]] = Evt[Set[Requirement]]()
  val demandNotViolated: default.Evt[Unit] = Evt[Unit]()
  val setTemperature: default.Evt[Double] = Evt[Double]()
  val resetTemperature: default.Evt[Unit] = Evt[Unit]()
  val newCostInformation: default.Evt[Unit] = Evt[Unit]()
  val makeTentativeOperator: default.Evt[NodeHost] = Evt[NodeHost]()

  var childHost1: Option[NodeHost] = None
  var childHost2: Option[NodeHost] = None

  val costSignal: Var[Map[Host, Map[Host, Cost]]] = Var(Map.empty[Host, Map[Host, Cost]])(ReSerializable.doNotSerialize, "cost") //information is unavailable due to decentralized nature

  val ready: Signal[Boolean] = Signal{qosInternal().size == numberOfChildren() && stage() == Stage.Measurement}

  val optimumHosts: default.Signal[Seq[NodeHost]] =  Signal{if(qosInternal().size == numberOfChildren() && stage() == Stage.Measurement)
    calculateOptimumHosts(children(), qosInternal(), childHost1, childHost2) else Seq.empty[NodeHost]}

  val adaptation: default.Event[Seq[NodeHost]] = demandViolated map { _ => optimumHosts()}

  adaptation observe{list => if(ready.now) adapt(list)}

  implicit val ordering: Ordering[(Duration, Double)] = new Ordering[(Duration, Double)] {
    def abs(x: Duration) = if (x < Duration.Zero) -x else x

    def compare(x: (Duration, Double), y: (Duration, Double)) = ((-x._1, x._2), (-y._1, y._2)) match {
      case ((d0, n0), (d1, n1)) if d0 == d1 && n0 == n1 => 0
      case ((d0, n0), (d1, n1)) if d0 < d1 && n0 < n1 => -1
      case ((d0, n0), (d1, n1)) if d0 > d1 && n0 > n1 => 1
      case ((d0, n0), (d1, n1)) =>
        math.signum((d0 - d1) / abs(d0 + d1) + (n0 - n1) / math.abs(n0 + n1)).toInt
    }
  }

  def calculateOptimumHosts(children: Map[NodeHost, Set[NodeHost]],
                            accumulatedCost: Map[NodeHost, Cost],
                            childHost1: Option[NodeHost],
                            childHost2: Option[NodeHost]): Seq[NodeHost]
  override def preStart(): Unit = {
    demandViolated.fire(Set.empty[Requirement])
    tick += {_ => {measureCosts(parentHosts)}}
    newCostInformation observe {_ => if(stage.now == Stage.Measurement) sendOutCostMessages(optimumHosts.now)}
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    startSimulation()
    startCostMeasurement()
  }

  override def startSimulation(): Unit = context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      if(optimizeFor == "latency"){
        hostProps = hostProps.advanceLatency
      } else if (optimizeFor == "bandwidth") {
        hostProps = hostProps.advanceBandwidth
      } else {
        hostProps = hostProps.advance
      }
      //measureCosts()
      reportCostsToNode()
    })

  override def receive = {
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
    case Node(actorRef) =>
      node = Some(actorRef)
      reportCostsToNode()
    case OptimizeFor(o) => optimizeFor = o
    case LatencyRequest(t)=>
      sender() ! LatencyResponse(t)
    case LatencyResponse(t) =>
      if(stage.now == Stage.Measurement){
        latencyResponses += sender()
        val latency = FiniteDuration(java.time.Duration.between(t, clock.instant()).dividedBy(2).toMillis, TimeUnit.MILLISECONDS)
        costs += hostMap(sender()) -> Cost(latency, costs(hostMap(sender())).bandwidth)
        costSignal.set(Map(thisHost -> costs))
        newCostInformation.fire()
      }
    case StartThroughPutMeasurement(instant) =>
      throughputStartMap += hostMap(sender()) -> (instant, clock.instant())
      throughputMeasureMap += hostMap(sender()) -> 0
    case TestEvent =>
      throughputMeasureMap += hostMap(sender()) -> (throughputMeasureMap(hostMap(sender())) + 1)
    case EndThroughPutMeasurement(instant, _) =>
      val senderDiff = java.time.Duration.between(throughputStartMap(hostMap(sender()))._1, instant)
      val receiverDiff = java.time.Duration.between(throughputStartMap(hostMap(sender()))._2, clock.instant())
      val calcBandwidth = (senderDiff.toMillis.toDouble / receiverDiff.toMillis.toDouble) * ((bandwidth.template.max / senderDiff.toMillis) * 100/*throughputMeasureMap(sender())*/)
      sender() ! ThroughPutResponse(calcBandwidth.toInt)
      //println(bandwidth, actual)
      throughputMeasureMap += hostMap(sender()) -> 0
    case ThroughPutResponse(r) =>
      if(stage.now == Stage.Measurement){
        costs += hostMap(sender()) -> Cost(costs(hostMap(sender())).duration, hostPropsToMap(hostMap(sender())).bandwidth)
        costSignal.set(Map(thisHost -> costs))
        bandwidthResponses += sender()
        //println("received response. Current Stage " + stage.now)
        newCostInformation.fire()
      }
    case gPE: PlacementEvent => processEvent(gPE, sender())
    case HostPropsRequest => sender() ! HostPropsResponse(costs)
    case _ =>
  }

  def processEvent(event: PlacementEvent, sender: ActorRef): Unit ={
    event match {

      /**Phase 0: Setup*/
      case BecomeActiveOperator(operator) => becomeActiveOperator(operator)
      case SetActiveOperator(operator) => setActiveOperator(operator)
      case ChildHost1(h) => receiveChildHost(hostMap(h))
      case ChildHost2(h1, h2) => receiveChildHosts(hostMap(h1), hostMap(h2))
      case ParentHost(p, ref) => receiveParentHost(hostMap(p), ref)

      /**Phase 1: Tentative Operator Selection*/
      case ChooseTentativeOperators(parents) => setUpTentatives(parents)
      case OperatorRequest => send(hostMap(sender), OperatorResponse(activeOperator, tentativeOperator))
      case OperatorResponse(ao, to) => processOperatorResponse(hostMap(sender), ao, to)

      case BecomeTentativeOperator(operator, p, pHosts, c1, c2) =>
        becomeTentativeOperator(operator, hostMap(sender), p, pHosts, c1, c2)
      case SetTemperature(t) =>
        setTemperature.fire(t)

      case TentativeAcknowledgement => processAcknowledgement(hostMap(sender))
      case ContinueSearching => collectOperatorInformation(hosts.now)

      case FinishedChoosing(tChildren) => childFinishedChoosingTentatives(hostMap(sender), tChildren)

      /**Phase 2: Measurement*/
      case m: CostMessage => processCostMessage(m, hostMap(sender))
      case RequirementsNotMet(requirements) => {demandViolated.fire(requirements)
       // println("adapt")
        }
      case RequirementsMet => demandNotViolated.fire()

      /**Phase 3: Migration*/
      case StateTransferMessage(o, p) => processStateTransferMessage(o, p)
      case MigrationComplete => completeMigration(hostMap(sender))
      case ChildResponse(c) => processChildResponse(hostMap(sender), c)

        /**Only for Annealing*/
      case ResetTemperature => resetTemperature.fire()
      case _ =>
    }
  }

  /**
  *
  * Phase 0: Setup
  *
  */

  def becomeActiveOperator(operator: ActiveOperator): Unit ={
    if(!isOperator){
      activeOperator = Some(operator)
      val temp = system.actorOf(activeOperator.get.props.withDeploy(Deploy(scope = remote.RemoteScope(self.path.address))))
      node = Some(temp)
      node.get ! Controller(self)

    }else{
      println("ERROR: Host already has an Operator")
    }
  }

  def setActiveOperator(props: Props): Unit ={
    if(!isOperator){
      activeOperator = Some(ActiveOperator(props, null))
    }else{
      println("ERROR: Host already has an Operator")
    }
  }

  def receiveChildHost(h: NodeHost): Unit = {
    children.transform(_ + (h -> Set.empty[NodeHost]))
    childHost1 = Some(h)
    send(h, ParentHost(self, node.get))
  }

  def receiveChildHosts(h1: NodeHost, h2: NodeHost): Unit = {
    children.transform(_ + (h1 -> Set.empty[NodeHost]))
    children.transform(_ + (h2 -> Set.empty[NodeHost]))
    childHost1 = Some(h1)
    childHost2 = Some(h2)
    send(h1, ParentHost(self, node.get))
    send(h2, ParentHost(self, node.get))
  }

  def receiveParentHost(p: NodeHost, ref: ActorRef): Unit = {
    hostToNodeMap += p -> ref
    if (node.isDefined) {
      reportCostsToNode()
      node.get ! Parent(ref)
      parentNode = Some(ref)
      parent = Some(p)
    }
  }

  /**
  *
  * Phase 1: Tentative Operator Selection
  *
  */

  def collectOperatorInformation(hosts: Set[NodeHost]) : Unit = {
    //println("setting UP", neighbors)
    operatorResponses = Set.empty[ActorRef]
    hosts.foreach(neighbor => send(neighbor, OperatorRequest))
  }

  def processOperatorResponse(sender: NodeHost, ao: Option[ActiveOperator], to: Option[TentativeOperator]): Unit = {
    operatorResponses += sender.actorRef
    if (ao.isDefined) {
      placement.transform(_ + (ao.get -> sender))
    } else if(to.isDefined) {
      placement.transform(_ + (to.get -> sender))
    }
    if (operatorResponses.size == hosts.now.size) {
      chooseTentativeOperators()
    }
  }

  def setUpTentatives(parents: Set[NodeHost]): Unit = {
    stage.set(Stage.TentativeOperatorSelection)
    parentHosts = parents
    if (parents.isEmpty) {
      consumer = true
      send(children.now.toSeq.head._1, ChooseTentativeOperators(Set(thisHost)))
    } else if (children.now.isEmpty) {
      parentHosts.foreach(send(_, FinishedChoosing(Set.empty[NodeHost])))
      stage.set(Stage.Measurement)
    } else {
      collectOperatorInformation(hosts.now)
    }
  }

  def chooseTentativeOperators() : Unit = {
    if (children.now.nonEmpty || parent.isDefined){
      if(activeOperator.isDefined){
        var timeout = 0
        var chosen: Boolean = false
        while (tentativeHosts.size < degree && timeout < 1000 && !chosen){
          val randomNeighbor =  hosts.now.toVector(random.nextInt(hosts.now.size))
          if(!reversePlacement.now.contains(randomNeighbor) && !tentativeHosts.contains(randomNeighbor)){
            val tenOp = TentativeOperator(activeOperator.get.props, activeOperator.get.dependencies)
            send(randomNeighbor, BecomeTentativeOperator(tenOp, parentNode.get, parentHosts, childHost1, childHost2))
            makeTentativeOperator.fire(randomNeighbor)
            chosen = true
          }
          timeout += 1
        }
        if(timeout >= 1000){
          //println("Not enough hosts available as tentative Operators. Continuing without...")
          send(children.now.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost))
        }
        //children.toSeq.head._1 ! ChooseTentativeOperators(tentativeHosts :+ self)
      } else {
        println("ERROR: Only Active Operator can choose Tentative Operators")
      }
    }
    else {
      stage.set(Stage.Measurement)
      parentHosts.foreach(send(_, FinishedChoosing(tentativeHosts)))
    }
  }

  def becomeTentativeOperator(operator: TentativeOperator, sender: NodeHost, p: ActorRef, pHosts: Set[NodeHost], c1 : Option[NodeHost], c2: Option[NodeHost]): Unit ={
    parentNode = Some(p)
    parentHosts = pHosts
    childHost1 = c1
    childHost2 = c2
    if(!isOperator){
      tentativeOperator = Some(operator)
      send(sender, TentativeAcknowledgement)
    } else {
      send(sender, ContinueSearching)
    }
  }

  def processAcknowledgement(sender: NodeHost): Unit = {
    tentativeHosts = tentativeHosts + sender
    if (tentativeHosts.size == degree) {
      send(children.now.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost))
    } else {
      chooseTentativeOperators()
    }
  }

  def childFinishedChoosingTentatives(sender: NodeHost, tChildren: Set[NodeHost]): Unit = {
    children.transform(_ + (sender -> tChildren))
    childrenCompletedChoosingTentatives += sender.actorRef
    if (activeOperator.isDefined) {
      if (childrenCompletedChoosingTentatives.size == children.now.size) {
        if (!consumer) {
          parentHosts.foreach(send(_, FinishedChoosing(tentativeHosts)))
        }
      } else if (childrenCompletedChoosingTentatives.size < children.now.size) {
        children.now.toSeq(childrenCompletedChoosingTentatives.size)._1.actorRef ! ChooseTentativeOperators(tentativeHosts + thisHost)
      }
    }
    if (childrenCompletedChoosingTentatives.size == children.now.size) {
      stage.set(Stage.Measurement)
    }
  }

  /**
    *
    * Phase 2: Measurement
    *
    * */

  override def measureCosts(hosts: Set[NodeHost]):Unit = {
    //println("measuring", hosts)
    val now = clock.instant()
    for (p <- hosts) {
      if (hostPropsToMap.contains(p)) {
        p.actorRef ! StartThroughPutMeasurement(now)
        context.system.scheduler.scheduleOnce(
          FiniteDuration((bandwidth.template.max / hostPropsToMap(p).bandwidth).toLong * 100, TimeUnit.MILLISECONDS),
          () => {
            p.actorRef ! EndThroughPutMeasurement(now.plusMillis(100), hostPropsToMap(p).bandwidth.toInt)
          })
        if (hostPropsToMap.contains(p)) {
          context.system.scheduler.scheduleOnce(
            FiniteDuration(hostPropsToMap(p).duration.toMillis * 2, TimeUnit.MILLISECONDS),
            () => {
              p.actorRef ! LatencyRequest(now)
            })
        } else {
          p.actorRef ! LatencyRequest(now)
        }
      }
    }
  }

  def sendOutCostMessages(adaptation: Seq[NodeHost]) : Unit = {
    if(stage.now == Stage.Measurement && (adaptation.nonEmpty || children.now.isEmpty)) {
      if (children.now.isEmpty && latencyResponses.size == parentHosts.size && bandwidthResponses.size == parentHosts.size) {
        parentHosts.foreach(parent => parent.actorRef ! CostMessage(costs(parent).duration, costs(parent).bandwidth))
      }
      else if (children.now.nonEmpty && processedCostMessages.size == numberOfChildren.now && latencyResponses.size == parentHosts.size && bandwidthResponses.size == parentHosts.size) {
        var bottleNeckNode = thisHost
        if (optimizeFor == "latency") {
          bottleNeckNode = minmaxBy(Maximizing, adaptation)(qosInternal.now.apply(_).duration)
        } else if (optimizeFor == "bandwidth") {
          bottleNeckNode = minmaxBy(Minimizing, adaptation)(qosInternal.now.apply(_).bandwidth)
        } else {
          bottleNeckNode = minmaxBy(Minimizing, adaptation)(x => (qosInternal.now.apply(x).duration, qosInternal.now.apply(x).bandwidth))
        }
        parentHosts.foreach(parent => parent.actorRef ! CostMessage(mergeLatency(qosInternal.now.apply(bottleNeckNode).duration, costs(parent).duration),
          mergeBandwidth(qosInternal.now.apply(bottleNeckNode).bandwidth, costs(parent).bandwidth)))
      }
    }
  }

  def processCostMessage(m: CostMessage, sender: NodeHost): Unit = {
    if(isOperator && isChild(sender)){
      qosInternal.transform(_.+(sender -> Cost(m.latency, m.bandwidth)))
      processedCostMessages += sender.actorRef
    }
  }

  /**
    *
    * Phase 3: Migration
    *
    */

  def adapt(optimumHosts: Seq[NodeHost]): Unit = {
    if (consumer) {
      broadcastMessage(StateTransferMessage(optimumHosts, node.get))
    }
    childHost1 = Option(optimumHosts.head)
    if(optimumHosts.size == 2){
      childHost2 = Option(optimumHosts.apply(1))
    }
    updateChildren(optimumHosts)
    stage.set(Stage.Migration)
  }

  def processStateTransferMessage(oHosts: Seq[NodeHost], p: ActorRef): Unit ={
    hostToNodeMap += hostMap(sender) -> p
    parent = Some(hostMap(sender))
    parentNode = Some(p)
    if(oHosts.contains(thisHost)){
      if(activeOperator.isDefined){
        reportCostsToNode()
        node.get ! Parent(parentNode.get)
        if(children.now.nonEmpty){
          broadcastMessage(StateTransferMessage(optimumHosts.now, node.get))
          adapt(optimumHosts.now)
        } else {
          stage.set(Stage.TentativeOperatorSelection)
          send(parent.get, MigrationComplete)
        }
      }
      if(tentativeOperator.isDefined){
        activate()
        reportCostsToNode()
        node.get ! Parent(parentNode.get)
        if(children.now.nonEmpty) {
          broadcastMessage(StateTransferMessage(optimumHosts.now, node.get))
          adapt(optimumHosts.now)
        } else {
          stage.set(Stage.TentativeOperatorSelection)
          send(parent.get, MigrationComplete)
        }
      }
      resetAllData(false)
      if(parent.isDefined && node.isDefined){
        send(parent.get, ChildResponse(node.get))
      }
    } else {
      //println("DEACTIVATING....")
      stage.set(Stage.TentativeOperatorSelection)
      resetAllData(true)
    }
  }

  def processChildResponse(sender: NodeHost, c: ActorRef): Unit = {
    if (childHost1.isDefined && sender.equals(childHost1.get)) {
      childNode1 = Some(c)
    } else if (childHost2.isDefined && sender.equals(childHost2.get)) {
      childNode2 = Some(c)
    } else {
      println("ERROR: Got Child Response from non child")
    }
    if (childNodes.size == children.now.size) {
      childNodes.size match {
        case 1 => node.get ! Child1(childNode1.get)
        case 2 => node.get ! Child2(childNode1.get, childNode2.get)
        case _ => println("ERROR: Got a Child Response but Node has no children")
      }
      childNode1 = None
      childNode2 = None
    }
  }

  def activate() : Unit = {
    //println("ACTIVATING...")
    if(tentativeOperator.isDefined){
      activeOperator = Some(ActiveOperator(tentativeOperator.get.props, tentativeOperator.get.dependencies))
      val temp = system.actorOf(activeOperator.get.props.withDeploy(Deploy(scope = remote.RemoteScope(self.path.address))))
      //println(temp)
      node = Some(temp)
      node.get ! Controller(self)
      tentativeOperator = None
    } else {
      println("ERROR: Cannot Activate without tentative Operator present")
    }
  }

  def resetAllData(deleteEverything: Boolean): Unit ={
    if(deleteEverything){
      if(node.isDefined){
        node.get ! Kill
      }
      node = None
      parent = None
      activeOperator = None
      tentativeOperator = None
      children.set(Map.empty[NodeHost, Set[NodeHost]])
      childHost1 = None
      childHost2 = None
    }

    qosInternal.set(Map.empty[NodeHost, Cost])

    parentHosts = Set.empty[NodeHost]

    tentativeHosts = Set.empty[NodeHost]

    placement.set(Map.empty[Operator, Host])

    childrenCompletedChoosingTentatives = Set.empty[ActorRef]
    childrenMigrated = Set.empty[ActorRef]
    processedCostMessages = Set.empty[ActorRef]
    operatorResponses = Set.empty[ActorRef]
    latencyResponses = Set.empty[ActorRef]
    bandwidthResponses = Set.empty[ActorRef]
  }

  def completeMigration(sender: NodeHost): Unit = {
    childrenMigrated += sender.actorRef
    if (parent.isDefined) {
      if (childrenMigrated.size == children.now.size) {
        stage.set(Stage.TentativeOperatorSelection)
        send(parent.get, MigrationComplete)
      }
    } else if (consumer) {
      if (childrenMigrated.size == children.now.size) {
        stage.set(Stage.TentativeOperatorSelection)
        resetAllData(false)
        send(children.now.toSeq.head._1, ChooseTentativeOperators(tentativeHosts + thisHost))
      }
    } else {
      println("ERROR: Something went terribly wrong")
    }
  }

  /**
    *
    * Helper Methods
    *
    */

  def childNodes : Seq[ActorRef] = {
    if(childNode1.isDefined && childNode2.isDefined){
      Seq(childNode1.get, childNode2.get)
    } else if(childNode1.isDefined){
      Seq(childNode1.get)
    } else if(childNode2.isDefined){
      Seq(childNode2.get)
    }else {
      Seq.empty[ActorRef]
    }
  }

  def broadcastMessage(message: PlacementEvent): Unit ={
    children.now.foreach(child => {
      send(child._1, message)
      child._2.foreach(tentativeChild => send(tentativeChild, message))
    })
  }

  private def isChild(host: NodeHost): Boolean ={
    var isChild = false
    children.now.foreach(child =>{
      if(child._1 == host || child._2.contains(host)){
        isChild = true
      }})
    isChild
  }

  def mergeBandwidth(b1: Double, b2: Double): Double = {
    Math.min(b1,b2)
  }

  def getPreviousChild(host: NodeHost, children: Map[NodeHost, Set[NodeHost]]): NodeHost= {
    children.foreach(child => if(child._1.equals(host) || child._2.contains(host)) return child._1)
    null
  }

  def getChildAndTentatives(host: NodeHost, children: Map[NodeHost, Set[NodeHost]]): Set[NodeHost] = {
    Set(host) ++ children(host)
  }

  def setNode(actorRef: ActorRef): Unit = {
    node = Some(actorRef)
    if(parentNode.isDefined){
      node.get ! Parent(parentNode.get)
    }
  }

  def mergeLatency(latency1: Duration, latency2: Duration): Duration ={
    latency1.+(latency2)
  }

  def updateChildren(optimumHosts: Seq[NodeHost]): Unit = {
    children.set(Map.empty[NodeHost, Set[NodeHost]])
    optimumHosts.foreach(host => children.transform(_ + (host -> Set.empty[NodeHost])))
  }

  def isOperator: Boolean ={
    activeOperator.isDefined || tentativeOperator.isDefined
  }

  def getOperator: Option[Operator] = {
    if(isOperator){
      if(activeOperator.isDefined){
        activeOperator
      }
      else tentativeOperator
    }
    else None
  }

  def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]): T = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U): T = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }
}
