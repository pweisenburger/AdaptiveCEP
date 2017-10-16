package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.qos.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.PathFinding._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.{Adaptation, Violation}
import com.lambdarookie.eventscala.backend.system._
import com.lambdarookie.eventscala.data.Queries._
import rescala._

/**
  * Created by monur.
  */
sealed trait System extends CEPSystem with QoSSystem

abstract class SystemImpl(val strategy: System => Event[Adaptation]) extends System {
  strategy(this) += { adaptation => replaceOperators(adaptation.assignments) }
  hosts.changed += { _ => forgetPaths() }
}





trait CEPSystem {
  protected val logging: Boolean

  /**
    * Select the best host for a given operator
    * @param operator Operator, whose host we are seeking
    * @return Selected host
    */
  def placeOperator(operator: Operator): Host



  private val operatorsVar: Var[Set[Operator]] = Var(Set.empty)
  private val hostsVar: Var[Set[Host]] = Var(Set.empty)
  private val nodesToOperatorsVar: Var[Map[ActorRef, Operator]] = Var(Map.empty)

  val operators: Signal[Set[Operator]] = operatorsVar
  val hosts: Signal[Set[Host]] = hostsVar
  val nodesToOperators: Signal[Map[ActorRef, Operator]] = nodesToOperatorsVar


  /**
    * Create and add an operator to the system's [[operators]] signal
    * @param id Id of the created operator
    * @param query Query of the created operator
    * @param outputs Parent operators of the created operator
    * @return The created operator
    */
  def createOperator(id: String, query: Query, outputs: Set[Operator]): Operator = {
    val op: Operator = query match {
      case q: LeafQuery => EventSourceImpl(id, this, q, outputs)
      case q: UnaryQuery => UnaryOperatorImpl(id, this, q, outputs)
      case q: BinaryQuery => BinaryOperatorImpl(id, this, q, outputs)
    }
    operatorsVar.transform(x => x + op)
    op
  }

  /**
    * Add a node-operator pair to the system's [[nodesToOperators]] signal
    * @param node ActorRef of a node as key
    * @param operator Operator as value
    */
  def addNodeOperatorPair(node: ActorRef, operator: Operator): Unit =
    nodesToOperatorsVar.transform(x => x + (node -> operator))

  def replaceOperators(assignments: Map[Operator, Host]): Unit =
    assignments.foreach { x =>
      x._1.asInstanceOf[OperatorImpl].move(x._2)
      if (logging) println(s"ADAPTATION:\t${x._1} is moved to ${x._2}")
    }

  /**
    * Get the host of a node. Every node is mapped to an operator and therefore a host
    * @param node Node, whose host we are seeking
    * @return Given node's host
    */
  def getHostByNode(node: ActorRef): Host = nodesToOperators.now.get(node) match {
    case Some(operator) => operator.host
    case None => throw new NoSuchElementException("ERROR: Following node is not defined in the system: " + node)
  }

  def addHosts(hosts: Set[Host]): Unit = hosts.foreach { host =>
    host.neighbors.foreach(_.asInstanceOf[HostImpl].neighbors += host)
    hostsVar.transform(_ + host)
  }

  def removeHosts(hosts: Set[Host]): Unit = hosts.foreach { host =>
    host.neighbors.foreach { n =>
      val hostImpl: HostImpl = n.asInstanceOf[HostImpl]
      hostImpl.neighbors -= host
      hostImpl.neighborLatencies -= host
      hostImpl.neighborBandwidths -= host
      hostImpl.neighborThroughputs -= host
    }
    hostsVar.transform(_ - host)
  }
}





trait QoSSystem {
  protected val logging: Boolean

  val priority: Priority

  def planAdaptation(violations: Set[Violation]): Set[Violation]


  private var paths: Set[Path] = Set.empty

  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandsViolated: Evt[Set[Violation]] = Evt[Set[Violation]]
  private val adaptingVar: Var[Option[Set[Violation]]] = Var(None)
  private val adaptingHelper: Signal[Option[Set[Violation]]] = Signal {
    if (queriesVar().exists(_.adapting().nonEmpty))
      Some(queriesVar().flatMap(_.adapting()).flatten)
    else
      None
  }

  protected val demandsViolated: Event[Set[Violation]] = fireDemandsViolated

  val violations: Signal[Set[Violation]] = Signal{ queriesVar().flatMap(_.violations()) }
  val waiting: Signal[Set[Violation]] = Signal { queriesVar().flatMap(_.waiting()) }
  val adapting: Signal[Option[Set[Violation]]] = adaptingVar


  demandsViolated += { vs =>
    val query: Query = vs.head.operator.query
    query.addViolations(vs)
    val adaptationPlanned: Set[Violation] = planAdaptation(vs)
    if (adaptationPlanned.nonEmpty) query.fireAdaptationPlanned(adaptationPlanned)
  }

  waiting.change += { diff =>
    if (diff.from.get.isEmpty && diff.to.get.nonEmpty) {
      if (logging) println(s"ADAPTATION:\t${diff.to.get} waiting adaptation")
      if (adaptingHelper.now.isEmpty) diff.to.get.map(_.operator.query).foreach(_.startAdapting())
    }
  }

  adaptingHelper.change += { diff =>
    val from: Option[Set[Violation]] = diff.from.get
    val to: Option[Set[Violation]] = diff.to.get
    if (from.isEmpty && to.isEmpty) {
      adaptingVar.transform(_ => None)
      if (logging) println(s"ADAPTATION:\tSystem is done adapting")
    } else if (from.isEmpty && to.nonEmpty) {
      if (logging) println(s"ADAPTATION:\tSystem is adapting to violations: ${to.get}")
      adaptingVar.transform(_ => to)
      to.get.foreach(_.operator.query.stopAdapting())
    } else if (from.nonEmpty && to.isEmpty) {
      adaptingVar.transform(_ => None)
      if (logging) println(s"ADAPTATION:\tSystem is done adapting")
      waiting.now.map(_.operator.query).foreach(_.startAdapting())
    }
  }


  private def updatePaths(path: Seq[Host]): Unit = {
    val path1 = path.init
    var path2 = path.tail
    paths ++ path1.flatMap { h1 =>
      val out: Set[Path] = path2.map { h2 => Path(h1, h2, path2.span(_ != h2)._1) }.toSet
      path2 = path2.tail
      out
    }
  }

  def forgetPaths(): Unit = paths = Set.empty

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  def fireDemandsViolated(violations: Set[Violation]): Unit =  fireDemandsViolated fire violations

  def getLatencyAndUpdatePaths(from: Host, to: Host, through: Option[Host] = None): TimeSpan =
    if (through.nonEmpty && through.get != to) {
      getLatencyAndUpdatePaths(from, through.get) + getLatencyAndUpdatePaths(through.get, to)
    } else {
      val found: Set[Path] = paths collect { case p@Path(`from`, `to`, _) => p }
      if (found.size == 1) {
        found.head.latency
      } else {
        if (found.size > 1) paths --= found // If there are duplicates it is en error. Remove them
        val bestPath: Path = priority.choosePath(from, to, paths)
        updatePaths(bestPath.toSeq)
        bestPath.latency
      }
    }

  def getBandwidthAndUpdatePaths(from: Host, to: Host, through: Option[Host] = None): BitRate =
    if (through.nonEmpty && through.get != to) {
      min(getBandwidthAndUpdatePaths(from, through.get), getBandwidthAndUpdatePaths(through.get, to))
    } else {
      val found: Set[Path] = paths collect { case p@Path(`from`, `to`, _) => p }
      if (found.size == 1) {
        found.head.bandwidth
      } else {
        if (found.size > 1) paths --= found // If there are duplicates it is en error. Remove them
        val bestPath: Path = priority.choosePath(from, to, paths)
        updatePaths(bestPath.toSeq)
        bestPath.bandwidth
      }
    }

  def getThroughputAndUpdatePaths(from: Host, to: Host, through: Option[Host] = None): BitRate =
    if (through.nonEmpty && through.get != to) {
      min(getThroughputAndUpdatePaths(from, through.get), getThroughputAndUpdatePaths(through.get, to))
    } else {
      val found: Set[Path] = paths collect { case p@Path(`from`, `to`, _) => p }
      if (found.size == 1) {
        found.head.throughput
      } else {
        if (found.size > 1) paths --= found // If there are duplicates it is en error. Remove them
        val bestPath: Path = priority.choosePath(from, to, paths)
        updatePaths(bestPath.toSeq)
        bestPath.throughput
      }
    }
}