package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.Operator

trait AverageFrequencyMonitor {

  val logging: Boolean
  val interval: Int

  var currentOutput: Option[Int] = None

  def onCreated[ND <: NodeData](nodeData: ND): Unit = {
    val query: Query = nodeData.query
    val requirements: Set[FrequencyRequirement] =
      query.requirements.collect{ case fr: FrequencyRequirement => fr }
    currentOutput = Some(0)
    if (requirements.nonEmpty) {
      nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          requirements.foreach(requirement => {
            require(requirement.ratio.timeSpan.getSeconds <= interval)
            // `divisor`, e.g., if `interval` == 30, and `requirement.ratio.timeSpan.getSeconds` == 10, then `divisor` == 3
            val divisor: Int = interval / requirement.ratio.timeSpan.getSeconds
            val frequency: Int = currentOutput.get / divisor
            if (logging) println(
              s"FREQUENCY:\tOn average, node `${nodeData.name}` emits $frequency events every " +
                s"${requirement.ratio.timeSpan.getSeconds} seconds. (Calculated every $interval seconds.)")
            val operator: Operator = nodeData.system.nodesToOperators.now.apply(nodeData.context.self)
            val instances = requirement.ratio.instances.getInstanceNum
            requirement.booleanOperator match {
              case Equal =>        if (!(frequency == instances)) query.addViolatedDemand(Violation(operator, requirement))
              case NotEqual =>     if (!(frequency != instances)) query.addViolatedDemand(Violation(operator, requirement))
              case Greater =>      if (!(frequency >  instances)) query.addViolatedDemand(Violation(operator, requirement))
              case GreaterEqual => if (!(frequency >= instances)) query.addViolatedDemand(Violation(operator, requirement))
              case Smaller =>      if (!(frequency <  instances)) query.addViolatedDemand(Violation(operator, requirement))
              case SmallerEqual => if (!(frequency <= instances)) query.addViolatedDemand(Violation(operator, requirement))
            }
          })
          currentOutput = Some(0)
        }
      )
    }
  }

  def onEventEmit(event: Event): Unit = {
    if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)
  }

}

case class AverageFrequencyLeafNodeMonitor(interval: Int, logging: Boolean)
  extends AverageFrequencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated[LeafNodeData](nodeData)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyUnaryNodeMonitor(interval: Int, logging: Boolean)
  extends AverageFrequencyMonitor with UnaryNodeMonitor {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated[UnaryNodeData](nodeData)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyBinaryNodeMonitor(interval: Int, logging: Boolean)
  extends AverageFrequencyMonitor with BinaryNodeMonitor {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated[BinaryNodeData](nodeData)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = AverageFrequencyLeafNodeMonitor(interval, logging)
  override def createUnaryNodeMonitor: UnaryNodeMonitor = AverageFrequencyUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = AverageFrequencyBinaryNodeMonitor(interval, logging)

}
