package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._

/**
  * Created by monur.
  */
class TestSystem extends System {
  override val hosts: Signal[Set[Host]] = createRandomHosts

  demandViolated += {v => v.operator.query.addViolatedDemand(v)}

  override def placeOperator(operator: Operator): Host = {
    val host: Host = if (hosts.now.exists(_.operators.now.isEmpty)) hosts.now.filter(_.operators.now.isEmpty).head
    else hosts.now.toVector((math.random * hosts.now.size).toInt)
    host.addOperator(operator)
    host
  }

  def getHostById(id: Int): Host = hosts.now.filter(h => h.asInstanceOf[TestHost].id == id).head

  private def createRandomHosts: Var[Set[Host]] = {
    val testHost1: TestHost = new TestHost(1, createRandomCoordinate, 1.gbps)
    val testHost2: TestHost = new TestHost(2, createRandomCoordinate, 1.gbps)
    val testHost3: TestHost = new TestHost(3, createRandomCoordinate, 1.gbps)
    val testHost4: TestHost = new TestHost(4, createRandomCoordinate, 1.gbps)

    testHost1.neighbors ++= Set(testHost2, testHost3)
    testHost2.neighbors ++= Set(testHost1, testHost4)
    testHost3.neighbors ++= Set(testHost1)
    testHost4.neighbors ++= Set(testHost2)

    val host1: Host = testHost1
    val host2: Host = testHost2
    val host3: Host = testHost3
    val host4: Host = testHost4

    host1.measureMetrics()
    host2.measureMetrics()
    host3.measureMetrics()
    host4.measureMetrics()

    Var(Set(host1, host2, host3, host4))
  }

  private def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)
}

class TestHost(val id: Int, val position: Coordinate, val maxBandwidth: BitRate) extends Host {
  var neighbors: Set[Host] = Set.empty

  override def measureFrequency(): Unit = lastFrequency = Ratio((math.random() * 10 + 5).toInt.instances, 5.sec)

  override def measureNeighborLatencies(): Unit =
    neighbors.foreach(n => lastLatencies += (n -> (Seq.empty, (math.random() * 5 + 1).toInt.ms)))

  override def measureNeighborBandwidths(): Unit =
    neighbors.foreach(n => lastBandwidths += (n -> (Seq.empty, (math.random() * 50 + 50).toInt.mbps)))

  override def measureNeighborThroughputs(): Unit = neighbors.foreach(n =>
    lastThroughputs += (n -> (Seq.empty,
      (math.random() * (if(lastBandwidths.contains(n)) lastBandwidths(n)._2.toKbps * 1024 else 0)).toInt.mbps)))

  override def toString: String = s"Host$id"
}