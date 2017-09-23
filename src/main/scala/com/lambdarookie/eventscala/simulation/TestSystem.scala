package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._

/**
  * Created by monur.
  */
case class TestSystem(logging: Boolean) extends System {
  override val hosts: Signal[Set[Host]] = createRandomHosts
  override val adaptation: Adaptation = Adaptation(strategy)

  if (logging) violations.change += { vs =>
    val from = vs.from.get
    val to = vs.to.get
    println(s"ADAPTATION:\tviolations changed from $from to $to")
  }

  override def placeOperator(operator: Operator): Host = {
    val host: Host = if (hosts.now.exists(_.operators.now.isEmpty))
      hosts.now.filter(_.operators.now.isEmpty).head
    else
      hosts.now.toVector((math.random * hosts.now.size).toInt)
    host.addOperator(operator)
    if (logging) println(s"LOG:\t\t$operator is placed on $host")
    host
  }

  override def isAdaptationPlanned(violations: Set[Violation]): Boolean = true

  def getHostById(id: Int): Host = hosts.now.filter(h => h.asInstanceOf[TestHost].id == id).head

  private def strategy(violations: Set[Violation]): Unit = {
    violations.foreach(_.demand match {
      case _: LatencyDemand =>
        println(s"ADAPTATION:\tLatency adaptation strategy")
      case _: BandwidthDemand =>
        println(s"ADAPTATION:\tBandwidth adaptation strategy")
      case _: ThroughputDemand =>
        println(s"ADAPTATION:\tThroughput adaptation strategy")
    })
    Thread.sleep(6000)
  }

  private def createRandomHosts: Var[Set[Host]] = {

    def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)

    val testHost1: TestHost = TestHost(1, createRandomCoordinate)
    val testHost2: TestHost = TestHost(2, createRandomCoordinate)
    val testHost3: TestHost = TestHost(3, createRandomCoordinate)
    val testHost4: TestHost = TestHost(4, createRandomCoordinate)
    val testHost5: TestHost = TestHost(5, createRandomCoordinate)

    testHost1.neighbors ++= Set(testHost2, testHost3)
    testHost2.neighbors ++= Set(testHost1, testHost3, testHost4)
    testHost3.neighbors ++= Set(testHost1, testHost2, testHost5)
    testHost4.neighbors ++= Set(testHost2)
    testHost5.neighbors ++= Set(testHost3)

    val host1: Host = testHost1
    val host2: Host = testHost2
    val host3: Host = testHost3
    val host4: Host = testHost4
    val host5: Host = testHost5

//    host1.neighborLatencies ++= Map(host2 -> 1.ms, host3 -> 1.ms)
//    host2.neighborLatencies ++= Map(host1 -> 10.ms, host3 -> 1.ms, host4 -> 1.ms)
//    host3.neighborLatencies ++= Map(host1 -> 1.ms, host2 -> 1.ms, host5 -> 1.ms)
//    host4.neighborLatencies ++= Map(host2 -> 1.ms)
//    host5.neighborLatencies ++= Map(host3 -> 1.ms)
//
//    host1.neighborBandwidths ++= Map(host2 -> 100.mbps, host3 -> 100.mbps)
//    host2.neighborBandwidths ++= Map(host1 -> 100.mbps, host3 -> 10.mbps, host4 -> 100.mbps)
//    host3.neighborBandwidths ++= Map(host1 -> 100.mbps, host2 -> 100.mbps, host5 -> 100.mbps)
//    host4.neighborBandwidths ++= Map(host2 -> 100.mbps)
//    host5.neighborBandwidths ++= Map(host3 -> 100.mbps)
//
//    host1.neighborThroughputs ++= Map(host2 -> 100.mbps, host3 -> 100.mbps)
//    host2.neighborThroughputs ++= Map(host1 -> 100.mbps, host3 -> 100.mbps, host4 -> 100.mbps)
//    host3.neighborThroughputs ++= Map(host1 -> 100.mbps, host2 -> 100.mbps, host5 -> 100.mbps)
//    host4.neighborThroughputs ++= Map(host2 -> 100.mbps)
//    host5.neighborThroughputs ++= Map(host3 -> 100.mbps)

    Var(Set(host1, host2, host3, host4, host5))
  }
}

case class TestHost(id: Int, position: Coordinate) extends Host {
  var neighbors: Set[Host] = Set.empty

  override def measureLatencyToNeighbor(neighbor: Host): TimeSpan = (math.random() * 5 + 1).toInt.ms

  override def measureBandwidthToNeighbor(neighbor: Host): BitRate = (math.random() * 50 + 50).toInt.mbps

  override def measureThroughputToNeighbor(neighbor: Host): BitRate = (math.random() *
    (if (neighborBandwidths.contains(neighbor))
      neighborBandwidths(neighbor).toMbps
    else
      0)).toInt.mbps

//  override def measureLatencyToNeighbor(neighbor: Host): TimeSpan = neighborLatencies(neighbor)
//
//  override def measureBandwidthToNeighbor(neighbor: Host): BitRate = neighborBandwidths(neighbor)
//
//  override def measureThroughputToNeighbor(neighbor: Host): BitRate = neighborThroughputs(neighbor)

  override def toString: String = s"Host$id"
}