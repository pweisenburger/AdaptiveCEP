package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.qos.QualityOfService
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._

/**
  * Created by monur.
  */
class TestSystem extends System {
  override val hosts: Signal[Set[Host]] = RandomHostFactory.createRandomHosts
  val operatorsVar: Var[Set[Operator]] = Var(Set())
  override val operators: Signal[Set[Operator]] = operatorsVar
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

    Var(Set(host1, host2, host3))
  }

  def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)
}

class TestHost(val id: Integer, val position: Coordinate) extends Host {
  val name: String = s"Host $id"

  var neighbors: Set[Host] = Set.empty
}