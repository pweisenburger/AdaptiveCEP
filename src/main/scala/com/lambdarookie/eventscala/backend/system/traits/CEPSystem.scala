package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.system.{BinaryOperatorImpl, EventSourceImpl, OperatorImpl, UnaryOperatorImpl}
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, LeafQuery, Query, UnaryQuery}
import rescala.{Signal, Var}

/**
  * Created by Ders.
  */
trait CEPSystem {
  protected val logging: Boolean

  /**
    * Select the best host for a given operator
    * @param operator Operator, whose host we are seeking
    * @return Selected host
    */
  def chooseHost(operator: Operator): Host



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
  private[backend] def createOperator(id: String, query: Query, outputs: Set[Operator]): Operator = {
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
  private[backend] def addNodeOperatorPair(node: ActorRef, operator: Operator): Unit =
    nodesToOperatorsVar.transform(x => x + (node -> operator))

  private[backend] def replaceOperators(assignments: Map[Operator, Host]): Unit =
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
