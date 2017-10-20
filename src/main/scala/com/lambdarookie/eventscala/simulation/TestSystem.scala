package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.backend.qos.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.PathFinding.Priority
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.simulation.Strategies.{measurePathBandwidth, measurePathLatency, measurePathThroughput, violatesNewDemands}
import rescala._

import scala.util.Random

/**
  * Created sortBy monur.
  */
case class TestSystem(override val strategy: System => Event[Adaptation], priority: Priority, logging: Boolean)
  extends SystemImpl(strategy) {

  addHosts(createRandomHosts)

  if (logging) violations.change += { vs =>
    val from = vs.from.get
    val to = vs.to.get
    if (from.diff(to).nonEmpty)
      println(s"ADAPTATION:\tViolations removed: ${from diff to}")
    if (to.diff(from).nonEmpty)
      println(s"ADAPTATION:\tViolations added: ${to diff from}")
  }

  override def chooseHost(operator: Operator): Host = {
    val hosts: Set[Host] = this.hosts.now
    val orderedFreeHosts: Seq[Host] =
      (hosts -- operators.now.map(_.host)).toSeq.sortWith(_.asInstanceOf[TestHost].id < _.asInstanceOf[TestHost].id)
    val host: Host = if (orderedFreeHosts.nonEmpty)
      orderedFreeHosts.head
    else
      hosts.iterator.drop(Random.nextInt(hosts.size)).next()
    if (logging) println(s"LOG:\t\t$operator is placed on $host")
    host
  }

  override def decideAdaptation(violations: Set[Violation]): Set[Violation] = okDecision(violations, hosts.now, operators.now)

  override def addHosts(hosts: Set[Host]): Unit = {
    super.addHosts(hosts)
    if (logging) println(s"LOG:\t\tHosts added: $hosts")
  }

  override def removeHosts(hosts: Set[Host]): Unit = {
    super.removeHosts(hosts)
    if (logging) if (logging) println(s"LOG:\t\tHosts removed: $hosts")
  }

  private def dummyDecision: Set[Violation] = Set.empty

  private def trivialDecision(violations: Set[Violation], hosts: Set[Host]): Set[Violation] = {
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
        if boMatcher(hosts, (h) => h.neighborLatencies - h, ld.booleanOperator, ld.timeSpan) => v
      case v@Violation(_, bd: BandwidthDemand)
        if boMatcher(hosts, (h) => h.neighborBandwidths - h, bd.booleanOperator, bd.bitRate) => v
      case v@Violation(_, td: ThroughputDemand)
        if boMatcher(hosts, (h) => h.neighborThroughputs - h, td.booleanOperator, td.bitRate) => v
    }
    if (logging && out.nonEmpty)
      println(s"ADAPTATION:\tSystem decided to try adapting to $out")
    if (logging && out.size < violations.size)
      println(s"ADAPTATION:\tSystem decided not to adapt to ${violations -- out}")
    out
  }

  private def okDecision(violations: Set[Violation], hosts: Set[Host], operators: Set[Operator]): Set[Violation] = {
    val out: Set[Violation] = violations.filter { v =>
      val descendants: Set[Operator] = v.operator.getDescendants
      val freeHosts: Set[Host] = hosts -- operators.map(_.host)
      v.demand match {
        case ld: LatencyDemand =>
          descendants.filter(o =>
            !isFulfilled(getLatencyAndUpdatePaths(o.host, v.operator.host), ld)).exists { vo =>
            freeHosts.exists { fh =>
              isFulfilled(getLatencyAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)) +
                          measurePathLatency(vo, this, Some(fh)), ld) &&
                !violatesNewDemands(this, vo.host, fh, operators)
            }
          }
        case bd: BandwidthDemand =>
          descendants.filter(o =>
            !isFulfilled(getBandwidthAndUpdatePaths(o.host, v.operator.host), bd)).exists { vo =>
            freeHosts.exists { fh =>
              isFulfilled(min(getBandwidthAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)),
                              measurePathBandwidth(vo, this, Some(fh))), bd) &&
                !violatesNewDemands(this, vo.host, fh, operators)
            }
          }
        case td: ThroughputDemand =>
          descendants.filter(o =>
            !isFulfilled(getThroughputAndUpdatePaths(o.host, v.operator.host), td)).exists { vo =>
            freeHosts.exists { fh =>
              isFulfilled(min(getThroughputAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)),
                          measurePathThroughput(vo, this, Some(fh))), td) &&
                !violatesNewDemands(this, vo.host, fh, operators)
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

    testHost1.neighbors ++= Set(testHost3)
    testHost2.neighbors ++= Set(testHost3, testHost8)
    testHost3.neighbors ++= Set(testHost1, testHost2, testHost4, testHost7, testHost8)
    testHost4.neighbors ++= Set(testHost3, testHost5, testHost7)
    testHost5.neighbors ++= Set(testHost4, testHost6, testHost7)
    testHost6.neighbors ++= Set(testHost5, testHost7)
    testHost7.neighbors ++= Set(testHost3, testHost4, testHost5, testHost6, testHost8)
    testHost8.neighbors ++= Set(testHost2, testHost3, testHost7)

    val out: Set[Host] = Set(testHost1, testHost2, testHost3, testHost4, testHost5, testHost6, testHost7, testHost8)
    checkNeighborhoodValidity(out)
    out
  }
}

case class TestHost(id: Int, position: Coordinate) extends HostImpl {
  override def measureLatencyToNeighbor(neighbor: Host): TimeSpan = Random.nextInt(10).ms

  override def measureBandwidthToNeighbor(neighbor: Host): BitRate = (Random.nextInt(50) + 55).mbps

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