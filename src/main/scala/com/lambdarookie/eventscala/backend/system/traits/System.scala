package com.lambdarookie.eventscala.backend.system.traits

import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
import com.lambdarookie.eventscala.data.Queries.Query
import rescala._

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

  /**
    * Select the best host for a given operator
    * @param operator Operator, whose host we are seeking
    * @return Selected host
    */
  def placeOperator(operator: Operator): Host

  /**
    * Get the host of a node. Every node is mapped to an operator and therefore a host
    * @param node Node, whose host we are seeking
    * @return Given node's host
    */
  def getHostByNode(node: ActorRef): Host = nodesToOperators.now.get(node) match {
    case Some(operator) => operator.host
    case None => throw new NoSuchElementException("ERROR: Following node is not defined in the system: " + node)
  }

  /**
    * Add a node-operator pair to the system's [[nodesToOperators]] signal
    * @param node ActorRef of a node as key
    * @param operator Operator as value
    */
  def addNodeOperatorPair(node: ActorRef, operator: Operator): Unit =
    nodesToOperatorsVar.transform(x => x + (node -> operator))

  /**
    * Add operator to the system's [[operators]] signal
    * @param operator Operator to add
    */
  def addOperator(operator: Operator): Unit = operatorsVar.transform(x => x + operator)

  /**
    * Measure the frequency of each host
    */
  def measureFrequencies(): Unit = hosts.now.foreach(_.measureFrequency())

  /**
    * Find the lowest latency between hosts using Dijkstra's shortest path algorithm
    */
  def measureLowestLatencies(): Unit = hosts.now.foreach(host => {
    host.measureNeighborLatencies()
    var dests = hosts.now - host
    var nexts = host.neighbors
    while(nexts.nonEmpty) {
      val n = nexts.head
      val inters: Seq[Host] = host.lastLatencies(n)._1 :+ n
      nexts = nexts.tail
      if(host.lastLatencies.contains(n) && dests.contains(n)) {
        dests -= n
        nexts ++= n.neighbors.intersect(dests)
        n.neighbors.foreach(nn => if(n.lastLatencies.contains(nn)
          && (!host.lastLatencies.contains(nn)
            || (host.lastLatencies.contains(nn)
              && host.lastLatencies(nn)._2 > host.lastLatencies(n)._2 + n.lastLatencies(nn)._2)))
          host.lastLatencies += (nn -> (inters ,host.lastLatencies(n)._2 + n.lastLatencies(nn)._2)))
      }
    }
  })

  /**
    * Find the highest bandwidth between hosts using a modified version of Dijkstra's algorithm
    */
  def measureHighestBandwidths(): Unit = hosts.now.foreach(host => {
    host.measureNeighborBandwidths()
    var dests = hosts.now - host
    var nexts = host.neighbors
    while(nexts.nonEmpty) {
      val n = nexts.head
      val inters: Seq[Host] = host.lastBandwidths(n)._1 :+ n
      nexts = nexts.tail
      if(host.lastBandwidths.contains(n) && dests.contains(n)) {
        dests -= n
        nexts ++= n.neighbors.intersect(dests)
        n.neighbors.foreach(nn => if(n.lastBandwidths.contains(nn)
          && (!host.lastBandwidths.contains(nn)
            || (host.lastBandwidths.contains(nn)
              &&  host.lastBandwidths(nn)._2 < min(host.lastBandwidths(n)._2, n.lastBandwidths(nn)._2))))
          host.lastBandwidths += (nn -> (inters, min(host.lastBandwidths(n)._2, n.lastBandwidths(nn)._2))))
      }
    }
  })

  /**
    * Find the highest throughput between hosts using a modified version of Dijkstra's algorithm
    */
  def measureHighestThroughputs(): Unit = hosts.now.foreach(host => {
    host.measureNeighborThroughputs()
    var dests = hosts.now - host
    var nexts = host.neighbors
    while(nexts.nonEmpty) {
      val n = nexts.head
      val inters: Seq[Host] = host.lastThroughputs(n)._1 :+ n
      nexts = nexts.tail
      if(host.lastThroughputs.contains(n) && dests.contains(n)) {
        dests -= n
        nexts ++= n.neighbors.intersect(dests)
        n.neighbors.foreach(nn => if(n.lastThroughputs.contains(nn)
          && (!host.lastThroughputs.contains(nn)
            || (host.lastThroughputs.contains(nn)
              && host.lastThroughputs(nn)._2 < min(host.lastThroughputs(n)._2, n.lastThroughputs(nn)._2))))
          host.lastThroughputs += (nn -> (inters, min(host.lastThroughputs(n)._2, n.lastThroughputs(nn)._2))))
      }
    }
  })
}


trait QoSSystem {
  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandViolated: Evt[Violation] = Evt[Violation]

  val queries: Signal[Set[Query]] = queriesVar
  val demandViolated: Event[Violation] = fireDemandViolated

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  def fireDemandViolated(violation: Violation): Unit = fireDemandViolated fire violation
}