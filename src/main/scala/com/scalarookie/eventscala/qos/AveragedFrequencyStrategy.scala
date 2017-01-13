package com.scalarookie.eventscala.qos

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorContext
import com.scalarookie.eventscala.caseclasses._

trait AveragedFrequencyStrategy {

  val logging: Boolean
  val interval: Int

  var currentOutput: Int = 0

  def onCreated(nodeName: String, query: Query, context: ActorContext): Unit = {
    if (query.frequencyRequirement.isDefined) {
      val frequencyRequirement = query.frequencyRequirement.get
      context.system.scheduler.schedule(
        initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = new Runnable {
          override def run(): Unit = {
            // `divisor`, e.g., if `interval` == 30, and `frequencyRequirement.seconds` == 10, then `divisor` == 3
            val divisor: Int = interval / frequencyRequirement.seconds
            val frequency: Int = currentOutput / divisor
            currentOutput = 0
            if (logging) println(s"FREQUENCY LOG:\t\tIn the last $interval seconds, $nodeName on average emitted $frequency events per ${frequencyRequirement.seconds} seconds.")
            frequencyRequirement.operator match {
              case Equal        => if (!(frequency == frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case NotEqual     => if (!(frequency != frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case Greater      => if (!(frequency >  frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case GreaterEqual => if (!(frequency >= frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case Smaller      => if (!(frequency <  frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case SmallerEqual => if (!(frequency <= frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
            }
          }
        }
      )
    }
  }

  def onEventEmit(): Unit = {
    currentOutput += 1
  }

}

case class AveragedFrequencyLeafNodeStrategy(interval: Int, logging: Boolean) extends AveragedFrequencyStrategy with LeafNodeStrategy {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit()

}

case class AveragedFrequencyUnaryNodeStrategy(interval: Int, logging: Boolean) extends AveragedFrequencyStrategy with UnaryNodeStrategy {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit()

}

case class AveragedFrequencyBinaryNodeStrategy(interval: Int, logging: Boolean) extends AveragedFrequencyStrategy with BinaryNodeStrategy {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit()

}

case class AveragedFrequencyStrategyFactory(interval: Int, logging: Boolean) extends StrategyFactory {

  override def getLeafNodeStrategy: LeafNodeStrategy = AveragedFrequencyLeafNodeStrategy(interval, logging)
  override def getUnaryNodeStrategy: UnaryNodeStrategy = AveragedFrequencyUnaryNodeStrategy(interval, logging)
  override def getBinaryNodeStrategy: BinaryNodeStrategy = AveragedFrequencyBinaryNodeStrategy(interval, logging)

}