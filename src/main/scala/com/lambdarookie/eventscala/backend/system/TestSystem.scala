package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.{Demand, Violation}

/**
  * Created by monur.
  */
class TestSystem extends System {
  override val hosts: Signal[Set[Host]] = RandomHostFactory.createRandomHosts
  override val demandViolated: Event[Violation] = null

  override def selectHostForOperator(operator: Operator): Host = hosts.now.toVector((math.random * hosts.now.size).toInt)
}

object RandomHostFactory {
  def createRandomHosts: Var[Set[Host]] = {
    val testHost1: TestHost = new TestHost(1, createRandomCoordinate, 1.gbps)
    val testHost2: TestHost = new TestHost(2, createRandomCoordinate, 1.gbps)
    val testHost3: TestHost = new TestHost(3, createRandomCoordinate, 1.gbps)
    val testHost4: TestHost = new TestHost(4, createRandomCoordinate, 1.gbps)

    testHost1.neighbors ++= Set(testHost2, testHost3)
    testHost2.neighbors ++= Set(testHost1, testHost3, testHost4)
    testHost3.neighbors ++= Set(testHost1, testHost2)
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

  def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)
}

class TestHost(val id: Integer, val position: Coordinate, val maxBandwidth: BitRate) extends Host {
  val name: String = s"Host $id"

  var neighbors: Set[Host] = Set.empty

  override def measureFrequency(): Unit = lastFrequency = Ratio((math.random() * 10 + 5).toInt.instances, 5.sec)

  override def measureNeighborLatencies(): Unit =
    neighbors.foreach(n => lastLatencies += (n -> (math.random() * 5 + 1).toInt.ms))

  override def measureNeighborBandwidths(): Unit =
    neighbors.foreach(n => lastBandwidths += (n -> (math.random() * 50 + 50).toInt.mbps))

  override def measureNeighborThroughputs(): Unit = neighbors.foreach(n => lastThroughputs +=
    (n -> (math.random() * (if(lastBandwidths.contains(n)) lastBandwidths(n).toKbps * 1024 else 0)).toInt.mbps))
}