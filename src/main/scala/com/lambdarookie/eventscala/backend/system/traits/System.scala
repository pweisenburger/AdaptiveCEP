package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
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
  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandViolated: Evt[Violation] = Evt[Violation]

  protected val demandViolated: Event[Violation] = fireDemandViolated

  val queries: Signal[Set[Query]] = queriesVar

  demandViolated += {v => v.operator.query.addViolatedDemand(v)}

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  def fireDemandViolated(violation: Violation): Unit = fireDemandViolated fire violation
}