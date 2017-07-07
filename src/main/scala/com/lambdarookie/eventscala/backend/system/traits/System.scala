package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.qos.QualityOfService
import com.lambdarookie.eventscala.backend.system.{BinaryOperator, EventSource, UnaryOperator}
import com.lambdarookie.eventscala.data.Queries.{BinaryQuery, LeafQuery, Query, UnaryQuery}
import com.lambdarookie.eventscala.graph.factory.OperatorFactory
import rescala._

import scala.collection.SortedSet

/**
  * Created by monur.
  */
trait System extends CEPSystem with QoSSystem {

}

trait CEPSystem {
  val hosts: Signal[Set[Host]]
  val operators: Signal[Set[Operator]]

  val nodesToOperatorsVar: Var[Map[ActorRef, Operator]] = Var(Map())
  val nodesToOperators: Signal[Map[ActorRef, Operator]] = nodesToOperatorsVar

  def selectHostForOperator(operator: Operator): Host = {
    // TODO: Operator placement strategy
    selectRandomHost
  }

  private def selectRandomHost: Host = hosts.now.toVector((math.random * hosts.now.size).toInt)

}

trait QoSSystem {
  val qos: Signal[Set[QualityOfService]]
  val demandViolated: Event[QualityOfService]
}

trait Host {
  val position: Coordinate
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

  def createChildOperator(id: String, subQuery: Query): Operator = OperatorFactory.createOperator(id, system, subQuery, Set(this))
}