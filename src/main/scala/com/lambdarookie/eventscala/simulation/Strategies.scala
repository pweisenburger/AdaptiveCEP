package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._

import scala.util.Random

/**
  * Created by monur.
  */
object Strategies {
  def dummyStrategy(system: System): Event[Adaptation] = Event {
    system.adapting().map { _ =>
      Adaptation(Map.empty)
    }
  }

  def strategy1(system: System): Event[Adaptation] = {
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

    Event {
      val operators: Set[Operator] = system.operators()
      val hosts: Set[Host] = system.hosts()
      val hostlessOperators: Set[Operator] = operators.filter(o => !hosts.contains(o.host))
      if (hostlessOperators.nonEmpty) {
        val freeHosts: Set[Host] = hosts -- operators.map(_.host)
        Some(Adaptation(hostlessOperators.map { o =>
          o -> (if (freeHosts.nonEmpty) freeHosts.head else hosts.iterator.drop(Random.nextInt(hosts.size)).next())
        }.toMap))
      } else {
        system.adapting().map { violations =>
          Adaptation(violations.flatMap { v =>
            val descendants: Set[Operator] = v.operator.getDescendants
            val freeHosts: Set[Host] = hosts -- operators.map(_.host)
            val hostChoices: Map[Operator, Set[Host]] = v.demand match {
              case ld: LatencyDemand =>
                //              println(s"ADAPTATION:\tLatency adaptation has begun")
                descendants.filter(o =>
                  !isFulfilled(system.getLatencyAndUpdatePaths(o.host, v.operator.host), ld)).map { vo =>
                  vo -> freeHosts.collect {
                    case fh if
                    isFulfilled(system.getLatencyAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), ld) &&
                      !violatesNewDemands(system, vo.host, fh, operators) => fh
                  }
                }.filter(_._2.nonEmpty).toMap
              case bd: BandwidthDemand =>
                //              println(s"ADAPTATION:\tBandwidth adaptation has begun")
                descendants.filter(o =>
                  !isFulfilled(system.getBandwidthAndUpdatePaths(o.host, v.operator.host), bd)).map { vo =>
                  vo -> freeHosts.collect {
                    case fh if
                    isFulfilled(system.getBandwidthAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), bd) &&
                      !violatesNewDemands(system, vo.host, fh, operators) => fh
                  }
                }.filter(_._2.nonEmpty).toMap
              case td: ThroughputDemand =>
                //              println(s"ADAPTATION:\tThroughput adaptation has begun")
                descendants.filter(o =>
                  !isFulfilled(system.getThroughputAndUpdatePaths(o.host, v.operator.host), td)).map { vo =>
                  vo -> freeHosts.collect {
                    case fh if
                    isFulfilled(system.getThroughputAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), td) &&
                      !violatesNewDemands(system, vo.host, fh, operators) => fh
                  }
                }.filter(_._2.nonEmpty).toMap
            }

            if (hostChoices.isEmpty) {
              //            println(s"ADAPTATION:\tNo right host could be found for the violating operators of $v. " +
              //              s"No replacement will be made.")
              Map.empty[Operator, Host]
            } else if (assignViolatingOperatorsIfPossible(hostChoices.head, hostChoices.tail)) {
              assignments
            } else {
              //            println(s"ADAPTATION:\tThere are not enough suitable hosts for every violating operator of $v. " +
              //              s"No replacement will be made.")
              Map.empty[Operator, Host]
            }
          }.toMap)
        }
      }
    }
  }

  def violatesNewDemands(system: System, oldHost: Host, newHost: Host, operators: Set[Operator]): Boolean = {
    val operatorsToDemands: Map[Operator, Set[Demand]] = operators.collect {
      case o if o.query.demands.nonEmpty => o -> o.query.demands
    }.toMap
    operatorsToDemands.exists { o_d =>
      o_d._2 exists {
        case ld: LatencyDemand =>
          isFulfilled(system.getLatencyAndUpdatePaths(oldHost, o_d._1.host), ld) &&
            !isFulfilled(system.getLatencyAndUpdatePaths(newHost, o_d._1.host), ld)
        case bd: BandwidthDemand =>
          isFulfilled(system.getBandwidthAndUpdatePaths(oldHost, o_d._1.host), bd) &&
            !isFulfilled(system.getBandwidthAndUpdatePaths(newHost, o_d._1.host), bd)
        case td: ThroughputDemand =>
          isFulfilled(system.getThroughputAndUpdatePaths(oldHost, o_d._1.host), td) &&
            !isFulfilled(system.getThroughputAndUpdatePaths(newHost, o_d._1.host), td)
      }
    }
  }
}
