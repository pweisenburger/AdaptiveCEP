package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.backend.qos.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.PathFinding.Priority
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._

import scala.util.Random

/**
  * Created sortBy monur.
  */
case class TestSystem(override val strategy: System => Event[Adaptation], priority: Priority, logging: Boolean)
  extends SystemImpl(strategy) {

  hostsVar.transform(_ => createRandomHosts )

  if (logging) violations.change += { vs =>
    val from = vs.from.get
    val to = vs.to.get
    if (from.diff(to).nonEmpty)
      println(s"ADAPTATION:\tViolations removed: ${from diff to}")
    if (to.diff(from).nonEmpty)
      println(s"ADAPTATION:\tViolations added: ${to diff from}")
  }

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
      val descendants: Set[Operator] = v.operator.getDescendants
      val freeHosts: Set[Host] = hosts -- operators.map(_.host)
      v.demand match {
        case ld: LatencyDemand =>
          descendants.filter(o =>
            !isFulfilled(getLatencyAndUpdatePaths(o.host, v.operator.host), ld)).exists { vo =>
            freeHosts.exists { fh =>
              isFulfilled(getLatencyAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), ld) &&
                !Strategies.violatesNewDemands(this, vo.host, fh, operators)
            }
          }
        case bd: BandwidthDemand =>
          descendants.filter(o =>
            !isFulfilled(getBandwidthAndUpdatePaths(o.host, v.operator.host), bd)).exists { vo =>
            freeHosts.exists { fh =>
              isFulfilled(getBandwidthAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), bd) &&
                !Strategies.violatesNewDemands(this, vo.host, fh, operators)
            }
          }
        case td: ThroughputDemand =>
          descendants.filter(o =>
            !isFulfilled(getThroughputAndUpdatePaths(o.host, v.operator.host), td)).exists { vo =>
            freeHosts.exists { fh =>
              isFulfilled(getThroughputAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), td) &&
                !Strategies.violatesNewDemands(this, vo.host, fh, operators)
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
      case v@Violation(_, ld: LatencyDemand)
        if boMatcher(hosts.now, (h) => h.neighborLatencies - h, ld.booleanOperator, ld.timeSpan) => v
      case v@Violation(_, bd: BandwidthDemand)
        if boMatcher(hosts.now, (h) => h.neighborBandwidths - h, bd.booleanOperator, bd.bitRate) => v
      case v@Violation(_, td: ThroughputDemand)
        if boMatcher(hosts.now, (h) => h.neighborThroughputs - h, td.booleanOperator, td.bitRate) => v
    }
    if (logging && out.nonEmpty)
      println(s"ADAPTATION:\tSystem decided to try adapting to $out")
    if (logging && out.size < violations.size)
      println(s"ADAPTATION:\tSystem decided not to adapt to ${violations -- out}")
    out
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
    val testHost8: TestHost = TestHost(8, createRandomCoordinate)
    val testHost9: TestHost = TestHost(9, createRandomCoordinate)
    val testHost10: TestHost = TestHost(10, createRandomCoordinate)
    val testHost11: TestHost = TestHost(11, createRandomCoordinate)
    val testHost12: TestHost = TestHost(12, createRandomCoordinate)
    val testHost13: TestHost = TestHost(13, createRandomCoordinate)

    testHost1.neighbors ++= Set(testHost2, testHost3, testHost12)
    testHost2.neighbors ++= Set(testHost1, testHost3, testHost4, testHost5, testHost6, testHost7, testHost10)
    testHost3.neighbors ++= Set(testHost1, testHost2, testHost10, testHost13)
    testHost4.neighbors ++= Set(testHost2)
    testHost5.neighbors ++= Set(testHost2)
    testHost6.neighbors ++= Set(testHost2, testHost8)
    testHost7.neighbors ++= Set(testHost2, testHost9, testHost10)
    testHost8.neighbors ++= Set(testHost6, testHost9, testHost11)
    testHost9.neighbors ++= Set(testHost7, testHost8)
    testHost10.neighbors ++= Set(testHost2, testHost3, testHost7)
    testHost11.neighbors ++= Set(testHost8)
    testHost12.neighbors ++= Set(testHost1)
    testHost13.neighbors ++= Set(testHost3)

//    testHost1.neighborLatencies ++= Map(testHost2 -> 10.ms, testHost3 -> 10.ms, testHost12 -> 10.ms)
//    testHost2.neighborLatencies ++= Map(testHost1 -> 10.ms, testHost3 -> 10.ms,
//      testHost4 -> 10.ms, testHost5 -> 10.ms, testHost6 -> 10.ms, testHost7 -> 10.ms, testHost10 -> 10.ms)
//    testHost3.neighborLatencies ++= Map(testHost1 -> 10.ms, testHost2 -> 10.ms, testHost10 -> 10.ms,
//      testHost13 -> 10.ms)
//    testHost4.neighborLatencies ++= Map(testHost2 -> 10.ms)
//    testHost5.neighborLatencies ++= Map(testHost2 -> 10.ms)
//    testHost6.neighborLatencies ++= Map(testHost2 -> 10.ms, testHost8 -> 10.ms)
//    testHost7.neighborLatencies ++= Map(testHost2 -> 10.ms, testHost9 -> 10.ms, testHost10 -> 10.ms)
//    testHost8.neighborLatencies ++= Map(testHost6 -> 10.ms, testHost9 -> 10.ms, testHost11 -> 10.ms)
//    testHost9.neighborLatencies ++= Map(testHost7 -> 10.ms, testHost8 -> 10.ms)
//    testHost10.neighborLatencies ++= Map(testHost2 -> 10.ms, testHost3 -> 10.ms, testHost7 -> 10.ms)
//    testHost11.neighborLatencies ++= Map(testHost8 -> 10.ms)
//    testHost12.neighborLatencies ++= Map(testHost1 -> 10.ms)
//    testHost13.neighborLatencies ++= Map(testHost3 -> 10.ms)
//
//    testHost1.neighborBandwidths ++= Map(testHost2 -> 100.mbps, testHost3 -> 100.mbps, testHost12 -> 100.mbps)
//    testHost2.neighborBandwidths ++= Map(testHost1 -> 100.mbps, testHost3 -> 100.mbps, testHost4 -> 100.mbps,
//      testHost5 -> 100.mbps, testHost6 -> 100.mbps, testHost7 -> 100.mbps, testHost10 -> 100.mbps)
//    testHost3.neighborBandwidths ++= Map(testHost1 -> 100.mbps, testHost2 -> 100.mbps, testHost10 -> 100.mbps,
//      testHost13 -> 100.mbps)
//    testHost4.neighborBandwidths ++= Map(testHost2 -> 100.mbps)
//    testHost5.neighborBandwidths ++= Map(testHost2 -> 100.mbps)
//    testHost6.neighborBandwidths ++= Map(testHost2 -> 100.mbps, testHost8 -> 100.mbps)
//    testHost7.neighborBandwidths ++= Map(testHost2 -> 100.mbps, testHost9 -> 100.mbps, testHost10 -> 100.mbps)
//    testHost8.neighborBandwidths ++= Map(testHost6 -> 100.mbps, testHost9 -> 100.mbps, testHost11 -> 100.mbps)
//    testHost9.neighborBandwidths ++= Map(testHost7 -> 100.mbps, testHost8 -> 100.mbps)
//    testHost10.neighborBandwidths ++= Map(testHost2 -> 100.mbps, testHost3 -> 100.mbps, testHost7 -> 100.mbps)
//    testHost11.neighborBandwidths ++= Map(testHost8 -> 100.mbps)
//    testHost12.neighborBandwidths ++= Map(testHost1 -> 100.mbps)
//    testHost13.neighborBandwidths ++= Map(testHost3 -> 100.mbps)
//
//    testHost1.neighborThroughputs ++= Map(testHost2 -> 70.mbps, testHost3 -> 70.mbps, testHost12 -> 70.mbps)
//    testHost2.neighborThroughputs ++= Map(testHost1 -> 70.mbps, testHost3 -> 70.mbps, testHost4 -> 70.mbps,
//      testHost5 -> 70.mbps, testHost6 -> 70.mbps, testHost7 -> 70.mbps, testHost10 -> 70.mbps)
//    testHost3.neighborThroughputs ++= Map(testHost1 -> 70.mbps, testHost2 -> 70.mbps, testHost10 -> 70.mbps,
//      testHost13 -> 70.mbps)
//    testHost4.neighborThroughputs ++= Map(testHost2 -> 70.mbps)
//    testHost5.neighborThroughputs ++= Map(testHost2 -> 70.mbps)
//    testHost6.neighborThroughputs ++= Map(testHost2 -> 70.mbps, testHost8 -> 70.mbps)
//    testHost7.neighborThroughputs ++= Map(testHost2 -> 70.mbps, testHost9 -> 70.mbps, testHost10 -> 70.mbps)
//    testHost8.neighborThroughputs ++= Map(testHost6 -> 70.mbps, testHost9 -> 70.mbps, testHost11 -> 70.mbps)
//    testHost9.neighborThroughputs ++= Map(testHost7 -> 70.mbps, testHost8 -> 70.mbps)
//    testHost10.neighborThroughputs ++= Map(testHost2 -> 70.mbps, testHost3 -> 70.mbps, testHost7 -> 70.mbps)
//    testHost11.neighborThroughputs ++= Map(testHost8 -> 70.mbps)
//    testHost12.neighborThroughputs ++= Map(testHost1 -> 70.mbps)
//    testHost13.neighborThroughputs ++= Map(testHost3 -> 70.mbps)

    val out: Set[Host] = Set(testHost1, testHost2, testHost3, testHost4, testHost5, testHost6, testHost7, testHost8,
      testHost9, testHost10, testHost11, testHost12, testHost13)
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