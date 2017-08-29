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
    * Calculate the path with the lowest latency between two hosts using Dijkstra's shortest path algorithm
    * @param source Source host
    * @param dest Destination host
    * @return The tuple of the path (For the path 'source -> A -> B -> dest' returns 'Seq(A, B, dest)') and the latency
    */
  def calculateLowestLatency(source: Host, dest: Host): (Seq[Host], TimeSpan) = {
    var visited: Set[Host] = Set(source)
    var latencies: Map[Host, (Seq[Host], TimeSpan)] = source.neighborLatencies - source
    var out: (Seq[Host], TimeSpan) = null
    while (latencies.nonEmpty) {
      latencies = latencies.toSeq.sortWith(_._2._2 < _._2._2).toMap
      val n: (Host, (Seq[Host], TimeSpan)) = latencies.head
      if (!visited.contains(n._1))
        if (n._1 == dest) {
          out = (n._2._1 :+ dest, n._2._2)
          latencies = Map.empty
        } else {
          visited += n._1
          val inters: Seq[Host] = n._2._1 :+ n._1
          n._1.neighborLatencies.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnLatency: TimeSpan = latencies(n._1)._2 + nn._2._2
            if (!latencies.contains(nn._1) || latencies(nn._1)._2 > sourceToNnLatency)
              latencies += nn._1 -> (inters, sourceToNnLatency)
          })
          latencies -= n._1
        }
    }
    out
  }

  /**
    * Calculate the path with the highest bandwidth between two hosts using a modified Dijkstra's shortest path algorithm
    * @param source Source host
    * @param dest Destination host
    * @return The tuple of the path (For the path 'source -> A -> B -> dest' returns 'Seq(A, B, dest)') and the bandwidth
    */
  def calculateHighestBandwidth(source: Host, dest: Host): (Seq[Host], BitRate) = {
    var visited: Set[Host] = Set(source)
    var bandwidths: Map[Host, (Seq[Host], BitRate)] = source.neighborBandwidths - source
    var out: (Seq[Host], BitRate) = null
    while (bandwidths.nonEmpty) {
      bandwidths = bandwidths.toSeq.sortWith(_._2._2 > _._2._2).toMap
      val n: (Host, (Seq[Host], BitRate)) = bandwidths.head
      if (!visited.contains(n._1))
        if (n._1 == dest) {
          out = (n._2._1 :+ dest, n._2._2)
          bandwidths = Map.empty
        } else {
          visited += n._1
          val inters: Seq[Host] = n._2._1 :+ n._1
          n._1.neighborBandwidths.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnBandwidth: BitRate = min(bandwidths(n._1)._2, nn._2._2)
            if (!bandwidths.contains(nn._1) || bandwidths(nn._1)._2 < sourceToNnBandwidth)
              bandwidths += nn._1 -> (inters, sourceToNnBandwidth)
          })
          bandwidths -= n._1
        }
    }
    out
  }

  /**
    * Calculate the path with the highest throughput between two hosts using a modified Dijkstra's shortest path algorithm
    * @param source Source host
    * @param dest Destination host
    * @return The tuple of the path (For the path 'source -> A -> B -> dest' returns 'Seq(A, B, dest)') and the throughput
    */
  def calculateHighestThroughput(source: Host, dest: Host): (Seq[Host], BitRate) = {
    var visited: Set[Host] = Set(source)
    var throughputs: Map[Host, (Seq[Host], BitRate)] = source.neighborThroughputs - source
    var out: (Seq[Host], BitRate) = null
    while (throughputs.nonEmpty) {
      throughputs = throughputs.toSeq.sortWith(_._2._2 > _._2._2).toMap
      val n: (Host, (Seq[Host], BitRate)) = throughputs.head
      if (!visited.contains(n._1))
        if (n._1 == dest) {
          out = (n._2._1 :+ dest, n._2._2)
          throughputs = Map.empty
        } else {
          visited += n._1
          val inters: Seq[Host] = n._2._1 :+ n._1
          n._1.neighborThroughputs.foreach(nn => if (!visited.contains(nn._1)) {
            val sourceToNnThroughput: BitRate = min(throughputs(n._1)._2, nn._2._2)
            if (!throughputs.contains(nn._1) || throughputs(nn._1)._2 < sourceToNnThroughput)
              throughputs += nn._1 -> (inters, sourceToNnThroughput)
          })
          throughputs -= n._1
        }
    }
    out
  }

  /**
    * Calculate the latency on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated latency as [[TimeSpan]]
    */
  def calculateLatency(path: Seq[Host]): TimeSpan = if (path.nonEmpty && path.tail.nonEmpty)
    path.head.neighborLatencies(path(1))._2 + calculateLatency(path.tail)
  else
    0.ms

  /**
    * Calculate the bandwidth on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated bandwidth as [[BitRate]]
    */
  def calculateBandwidth(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborBandwidths(path(1))._2, calculateBandwidth(path.tail))
  else
    Int.MaxValue.gbps

  /**
    * Calculate the throughput on a given path
    * @param path Path as a sequence of hosts
    * @return Calculated throughput as [[BitRate]]
    */
  def calculateThroughput(path: Seq[Host]): BitRate = if (path.nonEmpty && path.tail.nonEmpty)
    min(path.head.neighborThroughputs(path(1))._2, calculateThroughput(path.tail))
  else
    Int.MaxValue.gbps
}


trait QoSSystem {
  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandViolated: Evt[Violation] = Evt[Violation]

  val queries: Signal[Set[Query]] = queriesVar
  val demandViolated: Event[Violation] = fireDemandViolated

  demandViolated += {v => v.operator.query.addViolatedDemand(v)}

  def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  def fireDemandViolated(violation: Violation): Unit = fireDemandViolated fire violation
}