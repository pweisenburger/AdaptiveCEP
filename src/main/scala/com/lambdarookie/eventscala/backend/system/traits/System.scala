package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.data.Queries.Query
import com.lambdarookie.eventscala.graph.factory.OperatorFactory
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Requirement

import scala.collection.SortedSet

/**
  * Created by monur.
  */
trait System extends CEPSystem with QoSSystem


trait CEPSystem {
  val hosts: Signal[Set[Host]]

  private val operatorsVar: Var[Set[Operator]] = Var(Set.empty)
  private val nodesToOperatorsVar: Var[Map[ActorRef, Operator]] = Var(Map.empty)

  val operators: Signal[Set[Operator]] = operatorsVar
  val nodesToOperators: Signal[Map[ActorRef, Operator]] = nodesToOperatorsVar

  def selectHostForOperator(operator: Operator): Host = {
    // TODO: Operator placement strategy
    selectRandomHost
  }

  def getHostByNode(node: ActorRef): Host = nodesToOperators.now.get(node) match {
    case Some(operator) => operator.host
    case None => throw new NoSuchElementException("ERROR: Following node is not defined in the system: " + node)
  }

  def addNodeOperatorPair(node: ActorRef, operator: Operator): Unit =
    nodesToOperatorsVar.transform(x => x + (node -> operator))

  def addOperator(operator: Operator): Unit = operatorsVar.transform(x => x + operator)


  private def selectRandomHost: Host = hosts.now.toVector((math.random * hosts.now.size).toInt)
}


trait QoSSystem {
  private val queriesVar: Var[Set[Query]] = Var(Set.empty)

  val queries: Signal[Set[Query]] = queriesVar
  val demandViolated: Event[Requirement]

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)
}


trait Host {
  val position: Coordinate

  def neighbors: Set[Host]
  def measureNeighborLatencies(): Unit


  var lastLatencies: Map[Host, TimeSpan] = Map(this -> 0.ms)

  def sortNeighborsByProximity: SortedSet[Host] = {
    val sorted = SortedSet[Host]()((x: Host, y: Host) =>
      Ordering[Int].compare(position.calculateDistanceTo(x.position), position.calculateDistanceTo(y.position)))
    sorted ++ neighbors
  }
}


trait Operator {
  val testId: String
  val system: System
  val host: Host
  val query: Query
  val inputs: Seq[Operator]
  val outputs: Set[Operator]

  def createChildOperator(testId: String, subQuery: Query): Operator =
    OperatorFactory.createOperator(testId, system, subQuery, Set(this))
}