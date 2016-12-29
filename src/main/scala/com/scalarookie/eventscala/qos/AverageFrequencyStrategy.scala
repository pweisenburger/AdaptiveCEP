package com.scalarookie.eventscala.qos

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorContext
import com.scalarookie.eventscala.caseclasses._

class AverageFrequencyStrategy(interval: Int) extends FrequencyStrategy {

  var currentOutput: Int = 0

  override def onSubtreeCreated(context: ActorContext, nodeName: String, frequencyRequirement: FrequencyRequirement): Unit = {
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
            case Greater      => if (!(frequency >  frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
            case GreaterEqual => if (!(frequency >= frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
            case Smaller      => if (!(frequency <  frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
            case SmallerEqual => if (!(frequency <= frequencyRequirement.instances)) frequencyRequirement.callback(nodeName)
          }
        }
      }
    )
  }

  override def onEventEmit(context: ActorContext, nodeName: String, frequencyRequirement: FrequencyRequirement): Unit = {
    currentOutput += 1
  }

}
