package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QoSMetrics.Priority
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.Utilities
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._

import scala.util.Random

/**
  * Created sortBy monur.
  */
case class TestSystem(logging: Boolean, priority: Priority) extends System {
  override val hosts: Signal[Set[Host]] = Signal { createRandomHosts }
  override val adaptation: Adaptation = Adaptation(strategy)

  if (logging) violations.change += { vs => println(s"ADAPTATION:\tViolations changed to ${vs.to.get}") }

  override def placeOperator(operator: Operator): Host = {
    val hosts: Set[Host] = this.hosts.now
    val freeHosts: Set[Host] = hosts -- operators.now.map(_.host)
    val host: Host = if (freeHosts.nonEmpty)
      freeHosts.head
    else
      hosts.iterator.drop(Random.nextInt(hosts.size)).next()
    if (logging) println(s"LOG:\t\t$operator is placed on $host")
    host
  }

  override def planAdaptation(violations: Set[Violation]): Set[Violation] =
    planAdaptation1(violations, hosts.now, operators.now)

  private def planAdaptation1(violations: Set[Violation], hosts: Set[Host], operators: Set[Operator]): Set[Violation] = {
    val out: Set[Violation] = violations.filter { v =>
      val descendants: Set[Operator] = getDescendants(v.operator)
      val freeHosts: Set[Host] = hosts -- operators.map(_.host)
      v.demand match {
        case ld: LatencyDemand =>
          descendants.filter(o =>
            !Utilities.isFulfilled(getLatencyAndUpdatePaths(o.host, v.operator.host), ld)).exists { vo =>
            freeHosts.exists { fh =>
              Utilities.isFulfilled(getLatencyAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), ld) &&
                !violatesNewDemands(vo.host, fh, operators)
            }
          }
        case bd: BandwidthDemand =>
          descendants.filter(o =>
            !Utilities.isFulfilled(getBandwidthAndUpdatePaths(o.host, v.operator.host), bd)).exists { vo =>
            freeHosts.exists { fh =>
              Utilities.isFulfilled(getBandwidthAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), bd) &&
                !violatesNewDemands(vo.host, fh, operators)
            }
          }
        case td: ThroughputDemand =>
          descendants.filter(o =>
            !Utilities.isFulfilled(getThroughputAndUpdatePaths(o.host, v.operator.host), td)).exists { vo =>
            freeHosts.exists { fh =>
              Utilities.isFulfilled(getThroughputAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), td) &&
                !violatesNewDemands(vo.host, fh, operators)
            }
          }
      }
    }
    if (logging && out.nonEmpty)
      println(s"ADAPTATION:\tSystem decided to try adapting to $out")
    if (logging && out.size < violations.size)
      println(s"ADAPTATION:\tSystem decided not to adapt to ${violations -- out}")
    out
  }

  private def planAdaptation2(violations: Set[Violation]): Set[Violation] = {
    def boMatcher[T <: QoSUnit[T]](hosts: Set[Host],
                                metricMap: Host => Map[Host, T],
                                booleanOperator: BooleanOperator,
                                value: T): Boolean = booleanOperator match {
      case Equal => hosts.exists(h => metricMap(h).values.exists(_ == value))
      case NotEqual => hosts.exists(h => metricMap(h).values.exists(_ != value))
      case Greater => hosts.exists(h => metricMap(h).values.exists(_ > value))
      case GreaterEqual => hosts.exists(h => metricMap(h).values.exists(_ >= value))
      case Smaller => hosts.exists(h => metricMap(h).values.exists(_ < value))
      case SmallerEqual => hosts.exists(h => metricMap(h).values.exists(_ <= value))
    }

    val out: Set[Violation] = violations.collect {
      case v@Violation(_, LatencyDemand(bo, ts, _))
        if boMatcher(hosts.now, (h) => h.neighborLatencies - h, bo, ts) => v
      case v@Violation(_, BandwidthDemand(bo, br, _))
        if boMatcher(hosts.now, (h) => h.neighborBandwidths - h, bo, br) => v
      case v@Violation(_, ThroughputDemand(bo, br, _))
        if boMatcher(hosts.now, (h) => h.neighborThroughputs - h, bo, br) => v
    }
    if (logging) {
      println(s"ADAPTATION:\tSystem decided to try adapting to $out")
      println(s"ADAPTATION:\tSystem decided not to adapt to ${violations -- out}")
    }
    out
  }

  private def strategy(violations: Set[Violation]): Unit =
    strategyHelper(violations, hosts.now, operators.now, priority)

  private def strategyHelper(violations: Set[Violation],
                             hosts: Set[Host],
                             operators: Set[Operator],
                             priority: Priority): Unit = {
    var assignments: Map[Operator, Host] = Map.empty
    def assignViolatingOperatorsIfPossible(current: (Operator, Set[Host]),
                                           rest: Map[Operator, Set[Host]],
                                           taken: Set[Host] = Set.empty,
                                           assigned: Map[Operator, Host] = Map.empty): Boolean = {
      val choices: Set[Host] = current._2 -- taken
      if (choices.isEmpty) {
        false
      } else if (rest.isEmpty) {
        assignments = assigned + (current._1 -> choices.head)
        true
      } else {
        choices.exists { i =>
          assignViolatingOperatorsIfPossible(rest.head, rest.tail, taken + i, assigned + (current._1 -> i))
        }
      }
    }

    violations.foreach { v =>
      val descendants: Set[Operator] = getDescendants(v.operator)
      val freeHosts: Set[Host] = hosts -- operators.map(_.host)
      val hostChoices: Map[Operator, Set[Host]] = v.demand match {
        case ld: LatencyDemand =>
          println(s"ADAPTATION:\tLatency adaptation has begun")
          descendants.filter(o =>
            !Utilities.isFulfilled(getLatencyAndUpdatePaths(o.host, v.operator.host), ld)).map { vo =>
            vo -> freeHosts.collect {
              case fh if
              Utilities.isFulfilled(getLatencyAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), ld) &&
                !violatesNewDemands(vo.host, fh, operators) => fh
            }
          }.filter(_._2.nonEmpty).toMap
        case bd: BandwidthDemand =>
          println(s"ADAPTATION:\tBandwidth adaptation has begun")
          descendants.filter(o =>
            !Utilities.isFulfilled(getBandwidthAndUpdatePaths(o.host, v.operator.host), bd)).map { vo =>
            vo -> freeHosts.collect {
              case fh if
              Utilities.isFulfilled(getBandwidthAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), bd) &&
                !violatesNewDemands(vo.host, fh, operators) => fh
            }
          }.filter(_._2.nonEmpty).toMap
        case td: ThroughputDemand =>
          println(s"ADAPTATION:\tThroughput adaptation has begun")
          descendants.filter(o =>
            !Utilities.isFulfilled(getThroughputAndUpdatePaths(o.host, v.operator.host), td)).map { vo =>
            vo -> freeHosts.collect {
              case fh if
              Utilities.isFulfilled(getThroughputAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), td) &&
                !violatesNewDemands(vo.host, fh, operators) => fh
            }
          }.filter(_._2.nonEmpty).toMap
      }

      if (hostChoices.isEmpty)
        println(s"ADAPTATION:\tNo right host could be found for the violating operators of $v. " +
          s"No replacement will be made.")
      else if (assignViolatingOperatorsIfPossible(hostChoices.head, hostChoices.tail))
        replaceOperators(assignments)
      else
        println(s"ADAPTATION:\tThere are not enough suitable hosts for every violating operator of $v. " +
          s"No replacement will be made.")
    }
  }

  private def getDescendants(operator: Operator): Set[Operator] =
    operator.inputs.toSet ++ operator.inputs.flatMap(getDescendants)

  private def violatesNewDemands(oldHost: Host, newHost: Host, operators: Set[Operator]): Boolean = {
    val operatorsToDemands: Map[Operator, Set[Demand]] = operators.collect {
      case o if o.query.demands.nonEmpty => o -> o.query.demands
    }.toMap
    operatorsToDemands.exists { o_d =>
      o_d._2 exists {
        case ld: LatencyDemand => Utilities.isFulfilled(getLatencyAndUpdatePaths(oldHost, o_d._1.host), ld) &&
          !Utilities.isFulfilled(getLatencyAndUpdatePaths(newHost, o_d._1.host), ld)
        case bd: BandwidthDemand => Utilities.isFulfilled(getBandwidthAndUpdatePaths(oldHost, o_d._1.host), bd) &&
          !Utilities.isFulfilled(getBandwidthAndUpdatePaths(newHost, o_d._1.host), bd)
        case td: ThroughputDemand => Utilities.isFulfilled(getThroughputAndUpdatePaths(oldHost, o_d._1.host), td) &&
          !Utilities.isFulfilled(getThroughputAndUpdatePaths(newHost, o_d._1.host), td)
      }
    }
  }

  private def checkNeighborhoodValidity(hosts: Set[Host]): Unit = {
    hosts.foreach {h =>
      h.neighbors.foreach { n =>
        if (!n.neighbors.contains(h)) throw new IllegalStateException("Neighborhood must be a two-way relationship")
      }
    }
  }

  private def createRandomHosts: Set[Host] = {

    def createRandomCoordinate = Coordinate(-90 + math.random * 180, -180 + math.random * 360, math.random * 100)

    val testHost1: TestHost = TestHost(1, createRandomCoordinate)
    val testHost2: TestHost = TestHost(2, createRandomCoordinate)
    val testHost3: TestHost = TestHost(3, createRandomCoordinate)
    val testHost4: TestHost = TestHost(4, createRandomCoordinate)
    val testHost5: TestHost = TestHost(5, createRandomCoordinate)
    val testHost6: TestHost = TestHost(6, createRandomCoordinate)
    val testHost7: TestHost = TestHost(7, createRandomCoordinate)

    testHost1.neighbors ++= Set(testHost2, testHost3)
    testHost2.neighbors ++= Set(testHost1, testHost3, testHost4, testHost5, testHost6, testHost7)
    testHost3.neighbors ++= Set(testHost1, testHost2)
    testHost4.neighbors ++= Set(testHost2)
    testHost5.neighbors ++= Set(testHost2)
    testHost6.neighbors ++= Set(testHost2)
    testHost7.neighbors ++= Set(testHost2)

//    testHost1.neighborLatencies ++= Map(testHost2 -> 10.ms, testHost3 -> 10.ms)
//    testHost2.neighborLatencies ++= Map(testHost1 -> 10.ms, testHost3 -> 10.ms,
//      testHost4 -> 10.ms, testHost5 -> 10.ms, testHost6 -> 10.ms, testHost7 -> 10.ms)
//    testHost3.neighborLatencies ++= Map(testHost1 -> 10.ms, testHost2 -> 10.ms)
//    testHost4.neighborLatencies ++= Map(testHost2 -> 10.ms)
//    testHost5.neighborLatencies ++= Map(testHost2 -> 10.ms)
//    testHost6.neighborLatencies ++= Map(testHost2 -> 1.ms)
//    testHost7.neighborLatencies ++= Map(testHost2 -> 1.ms)
//
//    testHost1.neighborBandwidths ++= Map(testHost2 -> 100.mbps, testHost3 -> 100.mbps)
//    testHost2.neighborBandwidths ++= Map(testHost1 -> 100.mbps, testHost3 -> 100.mbps,
//      testHost4 -> 100.mbps, testHost5 -> 100.mbps, testHost6 -> 100.mbps, testHost7 -> 100.mbps)
//    testHost3.neighborBandwidths ++= Map(testHost1 -> 100.mbps, testHost2 -> 100.mbps)
//    testHost4.neighborBandwidths ++= Map(testHost2 -> 100.mbps)
//    testHost5.neighborBandwidths ++= Map(testHost2 -> 100.mbps)
//    testHost6.neighborBandwidths ++= Map(testHost2 -> 100.mbps)
//    testHost7.neighborBandwidths ++= Map(testHost2 -> 100.mbps)
//
//    testHost1.neighborThroughputs ++= Map(testHost2 -> 100.mbps, testHost3 -> 100.mbps)
//    testHost2.neighborThroughputs ++= Map(testHost1 -> 100.mbps, testHost3 -> 100.mbps,
//      testHost4 -> 100.mbps, testHost5 -> 100.mbps, testHost6 -> 100.mbps, testHost7 -> 100.mbps)
//    testHost3.neighborThroughputs ++= Map(testHost1 -> 100.mbps, testHost2 -> 100.mbps)
//    testHost4.neighborThroughputs ++= Map(testHost2 -> 100.mbps)
//    testHost5.neighborThroughputs ++= Map(testHost2 -> 100.mbps)
//    testHost6.neighborThroughputs ++= Map(testHost2 -> 100.mbps)
//    testHost7.neighborThroughputs ++= Map(testHost2 -> 100.mbps)

    val out: Set[Host] = Set(testHost1, testHost2, testHost3, testHost4, testHost5, testHost6, testHost7)
    checkNeighborhoodValidity(out)
    out
  }
}

case class TestHost(id: Int, position: Coordinate) extends HostImpl {
  override def measureLatencyToNeighbor(neighbor: Host): TimeSpan = (Random.nextInt(10) + 1).ms

  override def measureBandwidthToNeighbor(neighbor: Host): BitRate = (Random.nextInt(50) + 50).mbps

  override def measureThroughputToNeighbor(neighbor: Host): BitRate = (if (neighborBandwidths.contains(neighbor))
    Random.nextInt(neighborBandwidths(neighbor).toMbps.toInt)
  else
    Random.nextInt(50)).mbps

//  override def measureLatencyToNeighbor(neighbor: Host): TimeSpan = neighborLatencies(neighbor)
//
//  override def measureBandwidthToNeighbor(neighbor: Host): BitRate = neighborBandwidths(neighbor)
//
//  override def measureThroughputToNeighbor(neighbor: Host): BitRate = neighborThroughputs(neighbor)

  override def toString: String = s"Host$id"
}