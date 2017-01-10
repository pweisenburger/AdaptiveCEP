package com.scalarookie.eventscala.qos

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorContext
import com.scalarookie.eventscala.caseclasses._

abstract class FrequencyStrategy(interval: Int) {

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
            println(s"FREQUENCY:\t\tOn average, $nodeName emits $frequency events every ${frequencyRequirement.seconds} seconds.")
            frequencyRequirement.operator match {
              case Equal        => if (!(frequency == frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case NotEqual     => if (!(frequency != frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case Greater      => if (!(frequency > frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case GreaterEqual => if (!(frequency >= frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
              case Smaller      => if (!(frequency < frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
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

class FrequencyLeafNodeStrategy(interval: Int) extends FrequencyStrategy(interval) with LeafNodeStrategy {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit()

}

class FrequencyUnaryNodeStrategy(interval: Int) extends FrequencyStrategy(interval) with UnaryNodeStrategy {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit()

}

class FrequencyBinaryNodeStrategy(interval: Int) extends FrequencyStrategy(interval) with BinaryNodeStrategy {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit()

}
