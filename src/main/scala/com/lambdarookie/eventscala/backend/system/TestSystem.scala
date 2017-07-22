package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.qos.{Latency, QualityOfService}
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._
import com.lambdarookie.eventscala.backend.data.QoSUnits._

/**
  * Created by monur.
  */
class TestSystem extends System {
  override val hosts: Signal[Set[Host]] = RandomHostFactory.createRandomHosts
  override val qos: Signal[Set[QualityOfService]] = null
  override val demandViolated: Event[QualityOfService] = null
}

object RandomHostFactory {
  def createRandomHosts: Var[Set[Host]] = {
    val host1Impl: TestHost = new TestHost(1, createRandomCoordinate)
    val host2Impl: TestHost = new TestHost(2, createRandomCoordinate)
    val host3Impl: TestHost = new TestHost(3, createRandomCoordinate)

    host1Impl.neighbors ++= Set(host2Impl, host3Impl)
    host2Impl.neighbors ++= Set(host1Impl, host3Impl)
    host3Impl.neighbors ++= Set(host1Impl, host2Impl)

    val host1: Host = host1Impl
    val host2: Host = host2Impl
    val host3: Host = host3Impl

    host1.lastLatencies ++= Map(host2 -> Latency(host1, host2, 90.ms), host3 -> Latency(host1, host3, 91.ms))
    host2.lastLatencies ++= Map(host1 -> Latency(host2, host1, 92.ms), host3 -> Latency(host2, host3, 93.ms))
    host3.lastLatencies ++= Map(host1 -> Latency(host3, host1, 94.ms), host2 -> Latency(host3, host2, 95.ms))

    Var(Set(host1, host2, host3))
  }

  def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)
}

class TestHost(val id: Integer, val position: Coordinate) extends Host {
  val name: String = s"Host $id"

  var neighbors: Set[Host] = Set.empty
}