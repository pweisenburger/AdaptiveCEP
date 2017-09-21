package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.qos.QualityOfService.{Adaptation, Violation}
import com.lambdarookie.eventscala.backend.system._
import com.lambdarookie.eventscala.data.Queries._
import rescala._

/**
  * Created by monur.
  */
trait System extends CEPSystem with QoSSystem


trait CEPSystem {
  val hosts: Signal[Set[Host]]

  /**
    * Select the best host for a given operator
    * @param operator Operator, whose host we are seeking
    * @return Selected host
    */
  def placeOperator(operator: Operator): Host


  private val operatorsVar: Var[Set[Operator]] = Var(Set.empty)
  private val nodesToOperatorsVar: Var[Map[ActorRef, Operator]] = Var(Map.empty)

  val operators: Signal[Set[Operator]] = operatorsVar
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
      case q: LeafQuery => EventSource(id, this, q, outputs)
      case q: UnaryQuery => UnaryOperator(id, this, q, outputs)
      case q: BinaryQuery => BinaryOperator(id, this, q, outputs)
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

  /**
    * Get the host of a node. Every node is mapped to an operator and therefore a host
    * @param node Node, whose host we are seeking
    * @return Given node's host
    */
  def getHostByNode(node: ActorRef): Host = nodesToOperators.now.get(node) match {
    case Some(operator) => operator.host
    case None => throw new NoSuchElementException("ERROR: Following node is not defined in the system: " + node)
  }
}


trait QoSSystem {
  protected val logging: Boolean

  val adaptation: Adaptation

  def isAdaptationPlanned(violations: Set[Violation]): Boolean


  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandsViolated: Evt[Set[Violation]] = Evt[Set[Violation]]

  protected val demandsViolated: Event[Set[Violation]] = fireDemandsViolated

  val violations: Signal[Set[Violation]] = Signal{ queriesVar().flatMap(_.violations()) }
  val waiting: Signal[Set[Violation]] = Signal { queriesVar().flatMap(_.waiting()) }
  val adapting: Signal[Option[Set[Violation]]] = Signal {
    if (queriesVar().exists(_.adapting().nonEmpty))
      Some(queriesVar().flatMap(_.adapting()).flatten)
    else
      None
  }

  demandsViolated += { vs =>
    val query: Query = if (vs.map(_.operator.query).size == 1)
      vs.head.operator.query
     else
      throw new RuntimeException(s"ERROR: Every violation must belong to the same query")
    vs.foreach(query.addViolation)
    if (isAdaptationPlanned(vs)) query.fireAdaptationPlanned(vs)
  }
  waiting.change += { diff =>
    val from: Set[Violation] = diff.from.get
    val to: Set[Violation] = diff.to.get
    if (from.isEmpty && to.nonEmpty) {
      if (logging) println(s"ADAPTATION:\t$to waiting adaptation")
      if (adapting.now.isEmpty)  to.map(_.operator.query).foreach(_.startAdapting())
    }
  }
  adapting.change += {diff =>
    val from: Option[Set[Violation]] = diff.from.get
    val to: Option[Set[Violation]] = diff.to.get
    if (from.isEmpty) {
      if (logging) println(s"ADAPTATION:\tSystem is adapting violations: ${to.get}")
      adaptation.strategy(to.get)
      if (to.get.isEmpty)
        queriesVar.now.foreach(_.stopAdapting())
      else
        to.get.foreach(_.operator.query.stopAdapting())
    } else if (from.nonEmpty && to.nonEmpty) {
      if (logging) println(s"ADAPTATION:\tSystem is already adapting. Changed from $from to $to")
    } else {
      if (logging) println(s"ADAPTATION:\tSystem is done adapting")
      val vsQueue: Set[Violation] = waiting.now
      if (vsQueue.nonEmpty) vsQueue.map(_.operator.query).foreach(_.startAdapting())
    }
  }

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  def fireDemandsViolated(violations: Set[Violation]): Unit =  fireDemandsViolated fire violations
}