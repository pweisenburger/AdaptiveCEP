package com.lambdarookie.eventscala.graph.nodes.traits

import akka.actor.{Actor, ActorRef}
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.monitors._
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.graph.factory.NodeFactory

trait Node extends Actor {

  val name: String = self.path.name
  val testId: String

  val system: System
  val query: Query
  val operator: Operator
  val publishers: Map[String, ActorRef]
  val frequencyMonitorFactory: MonitorFactory
  val latencyMonitorFactory: MonitorFactory

  system.addNodeOperatorPair(self, operator)

  def createChildNode(id: Int, query: Query, childOperator: Operator): ActorRef =
    NodeFactory.createNode(
      system, context, query, childOperator, publishers,
      frequencyMonitorFactory, latencyMonitorFactory, None, None, s"$name-$id-", s"$testId-$id")

}
