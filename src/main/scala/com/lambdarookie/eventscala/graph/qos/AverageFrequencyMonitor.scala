package com.lambdarookie.eventscala.graph.qos

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorContext
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._

trait AverageFrequencyMonitor {

  val logging: Boolean
  val interval: Int

  var currentOutput: Option[Int] = None

  def onCreated(name: String, query: Query, context: ActorContext): Unit = {
    val requirements: Set[FrequencyRequirement] = query.requirements.collect{ case fr: FrequencyRequirement => fr }
    val callbackNodeData: NodeData = NodeData(name, query, context)
    currentOutput = Some(0)
    if (requirements.nonEmpty) {
      context.system.scheduler.schedule(
        initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          requirements.foreach(requirement => {
            require(requirement.seconds <= interval)
            // `divisor`, e.g., if `interval` == 30, and `requirement.seconds` == 10, then `divisor` == 3
            val divisor: Int = interval / requirement.seconds
            val frequency: Int = currentOutput.get / divisor
            if (logging) println(
              s"FREQUENCY:\tOn average, node `$name` emits $frequency events every ${requirement.seconds} seconds." +
              s"(Calculated every $interval seconds.)")
            requirement.operator match {
              case Equal =>        if (!(frequency == requirement.instances)) requirement.callback(callbackNodeData)
              case NotEqual =>     if (!(frequency != requirement.instances)) requirement.callback(callbackNodeData)
              case Greater =>      if (!(frequency >  requirement.instances)) requirement.callback(callbackNodeData)
              case GreaterEqual => if (!(frequency >= requirement.instances)) requirement.callback(callbackNodeData)
              case Smaller =>      if (!(frequency <  requirement.instances)) requirement.callback(callbackNodeData)
              case SmallerEqual => if (!(frequency <= requirement.instances)) requirement.callback(callbackNodeData)
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

case class AverageFrequencyLeafNodeMonitor(interval: Int, logging: Boolean) extends AverageFrequencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyUnaryNodeMonitor(interval: Int, logging: Boolean) extends AverageFrequencyMonitor with UnaryNodeMonitor {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyBinaryNodeMonitor(interval: Int, logging: Boolean) extends AverageFrequencyMonitor with BinaryNodeMonitor {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = AverageFrequencyLeafNodeMonitor(interval, logging)
  override def createUnaryNodeMonitor: UnaryNodeMonitor = AverageFrequencyUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = AverageFrequencyBinaryNodeMonitor(interval, logging)

}
