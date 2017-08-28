package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._

/**
  * Created by monur.
  */
case class TestSystem(logging: Boolean) extends System {

  override val hosts: Signal[Set[Host]] = createRandomHosts

  demandViolated += {v => v.operator.query.addViolatedDemand(v)}

  override def placeOperator(operator: Operator): Host = {
    val host: Host = if (hosts.now.exists(_.operators.now.isEmpty))
      hosts.now.filter(_.operators.now.isEmpty).head
    else
      hosts.now.toVector((math.random * hosts.now.size).toInt)
    host.addOperator(operator)
    if (logging) println(s"LOG:\t$operator is placed on $host")
    host
  }

  def getHostById(id: Int): Host = hosts.now.filter(h => h.asInstanceOf[TestHost].id == id).head

  private def createRandomHosts: Var[Set[Host]] = {
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

//    host1.neighborLatencies ++= Map(host2 -> (Seq(), 1.ms), host3 -> (Seq(), 1.ms))
//    host2.neighborLatencies ++= Map(host1 -> (Seq(), 1.ms), host3 -> (Seq(), 1.ms), host4 -> (Seq(), 1.ms))
//    host3.neighborLatencies ++= Map(host1 -> (Seq(), 100.ms), host2 -> (Seq(), 1.ms))
//    host4.neighborLatencies ++= Map(host2 -> (Seq(), 1.ms))
//
//    host1.neighborBandwidths ++= Map(host2 -> (Seq(), 100.mbps), host3 -> (Seq(), 100.mbps))
//    host2.neighborBandwidths ++= Map(host1 -> (Seq(), 10.mbps), host3 -> (Seq(), 100.mbps), host4 -> (Seq(), 100.mbps))
//    host3.neighborBandwidths ++= Map(host1 -> (Seq(), 100.mbps), host2 -> (Seq(), 100.mbps))
//    host4.neighborBandwidths ++= Map(host2 -> (Seq(), 100.mbps))
//
//    host1.neighborThroughputs ++= Map(host2 -> (Seq(), 50.mbps), host3 -> (Seq(), 30.mbps))
//    host2.neighborThroughputs ++= Map(host1 -> (Seq(), 50.mbps), host4 -> (Seq(), 10.mbps))
//    host3.neighborThroughputs ++= Map(host1 -> (Seq(), 30.mbps))
//    host4.neighborThroughputs ++= Map(host2 -> (Seq(), 10.mbps))

    Var(Set(host1, host2, host3, host4))
  }

  private def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)
}

class TestHost(val id: Int, val position: Coordinate, val maxBandwidth: BitRate) extends Host {
  var neighbors: Set[Host] = Set.empty

  override def measureFrequency(): Ratio = Ratio((math.random() * 10 + 5).toInt.instances, 5.sec)

    override def measureNeighborLatencies(): Unit = neighbors.foreach(n =>
      neighborLatencies += (n -> (Seq.empty, (math.random() * 5 + 1).toInt.ms)))


    override def measureNeighborBandwidths(): Unit = neighbors.foreach(n =>
      neighborBandwidths += (n -> (Seq.empty, (math.random() * 50 + 50).toInt.mbps)))

  override def measureNeighborThroughputs(): Unit = neighbors.foreach(n =>
    neighborThroughputs += (n -> (Seq.empty, (math.random() * (
      if (neighborBandwidths.contains(n))
        neighborBandwidths(n)._2.toMbps
      else
        0)
      ).toInt.mbps)))

//  override def measureNeighborLatencies(): Unit = {}
//
//  override def measureNeighborBandwidths(): Unit = {}
//
//  override def measureNeighborThroughputs(): Unit = {}

  override def toString: String = s"Host$id"
}