package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.backend.qos.QoSMetrics.Priority
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.Utilities
import com.lambdarookie.eventscala.backend.system.traits._
import rescala._

/**
  * Created by Ders.
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
      system.adapting().map { violations =>
        Adaptation(violations.flatMap { v =>
          val descendants: Set[Operator] = Utilities.getDescendants(v.operator)
          val operators: Set[Operator] = system.operators()
          val freeHosts: Set[Host] = system.hosts() -- operators.map(_.host)
          val hostChoices: Map[Operator, Set[Host]] = v.demand match {
            case ld: LatencyDemand =>
//              println(s"ADAPTATION:\tLatency adaptation has begun")
              descendants.filter(o =>
                !Utilities.isFulfilled(system.getLatencyAndUpdatePaths(o.host, v.operator.host), ld)).map { vo =>
                vo -> freeHosts.collect {
                  case fh if
                  Utilities.isFulfilled(system.getLatencyAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), ld) &&
                    !Utilities.violatesNewDemands(system, vo.host, fh, operators) => fh
                }
              }.filter(_._2.nonEmpty).toMap
            case bd: BandwidthDemand =>
//              println(s"ADAPTATION:\tBandwidth adaptation has begun")
              descendants.filter(o =>
                !Utilities.isFulfilled(system.getBandwidthAndUpdatePaths(o.host, v.operator.host), bd)).map { vo =>
                vo -> freeHosts.collect {
                  case fh if
                  Utilities.isFulfilled(system.getBandwidthAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), bd) &&
                    !Utilities.violatesNewDemands(system, vo.host, fh, operators) => fh
                }
              }.filter(_._2.nonEmpty).toMap
            case td: ThroughputDemand =>
//              println(s"ADAPTATION:\tThroughput adaptation has begun")
              descendants.filter(o =>
                !Utilities.isFulfilled(system.getThroughputAndUpdatePaths(o.host, v.operator.host), td)).map { vo =>
                vo -> freeHosts.collect {
                  case fh if
                  Utilities.isFulfilled(system.getThroughputAndUpdatePaths(fh, v.operator.host, Some(vo.outputs.head.host)), td) &&
                    !Utilities.violatesNewDemands(system, vo.host, fh, operators) => fh
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
