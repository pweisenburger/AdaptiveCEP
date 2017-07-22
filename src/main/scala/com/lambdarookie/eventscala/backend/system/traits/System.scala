package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.qos.{Latency, QualityOfService}
import com.lambdarookie.eventscala.data.Queries.Query
import com.lambdarookie.eventscala.graph.factory.OperatorFactory
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._

import scala.collection.SortedSet

/**
  * Created by monur.
  */
trait System extends CEPSystem with QoSSystem

trait CEPSystem {
  val hosts: Signal[Set[Host]]

  val operatorsVar: Var[Set[Operator]] = Var(Set.empty)
  val operators: Signal[Set[Operator]] = operatorsVar

  val nodesToOperatorsVar: Var[Map[ActorRef, Operator]] = Var(Map.empty)
  val nodesToOperators: Signal[Map[ActorRef, Operator]] = nodesToOperatorsVar

  def selectHostForOperator(operator: Operator): Host = {
    // TODO: Operator placement strategy
    selectRandomHost
  }

  def getHostByNode(node: ActorRef): Option[Host] = nodesToOperators.now.get(node) match {
    case Some(operator) => Some(operator.host)
    case None => None
  }


  private def selectRandomHost: Host = hosts.now.toVector((math.random * hosts.now.size).toInt)
}

trait QoSSystem {
  val qos: Signal[Set[QualityOfService]]
  val demandViolated: Event[QualityOfService]
}

trait Host {
  val position: Coordinate

  var lastLatencies: Map[Host, Latency] = Map(this -> Latency(this, this, 0.ms))


  def neighbors: Set[Host]

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

  def createChildOperator(testId: String, subQuery: Query): Operator = OperatorFactory.createOperator(testId, system, subQuery, Set(this))
}