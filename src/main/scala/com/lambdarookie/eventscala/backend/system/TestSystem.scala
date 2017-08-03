package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Requirement

/**
  * Created by monur.
  */
class TestSystem extends System {
  override val hosts: Signal[Set[Host]] = RandomHostFactory.createRandomHosts
  override val demandViolated: Event[Requirement] = null

  override def selectHostForOperator(operator: Operator): Host = hosts.now.toVector((math.random * hosts.now.size).toInt)
}

object RandomHostFactory {
  def createRandomHosts: Var[Set[Host]] = {
    val testHost1: TestHost = new TestHost(1, createRandomCoordinate)
    val testHost2: TestHost = new TestHost(2, createRandomCoordinate)
    val testHost3: TestHost = new TestHost(3, createRandomCoordinate)
    val testHost4: TestHost = new TestHost(4, createRandomCoordinate)

    testHost1.neighbors ++= Set(testHost2, testHost3)
    testHost2.neighbors ++= Set(testHost1, testHost3, testHost4)
    testHost3.neighbors ++= Set(testHost1, testHost2)
    testHost4.neighbors ++= Set(testHost2)

    val host1: Host = testHost1
    val host2: Host = testHost2
    val host3: Host = testHost3
    val host4: Host = testHost4

    host1.measureNeighborLatencies()
    host2.measureNeighborLatencies()
    host3.measureNeighborLatencies()
    host4.measureNeighborLatencies()

    Var(Set(host1, host2, host3, host4))
  }

  def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)
}

class TestHost(val id: Integer, val position: Coordinate) extends Host {
  val name: String = s"Host $id"

  var neighbors: Set[Host] = Set.empty

  override def measureNeighborLatencies(): Unit =
    neighbors.foreach(n => lastLatencies += (n -> (math.random() * 6).toInt.ms))

}