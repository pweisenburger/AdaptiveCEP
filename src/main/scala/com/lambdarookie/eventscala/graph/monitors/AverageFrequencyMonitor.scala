package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.Host
import com.lambdarookie.eventscala.data.Events._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

case class AverageFrequencyMonitor(interval: Int, logging: Boolean, testing: Boolean) extends NodeMonitor {
  var currentOutput: Option[Int] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val host: Host = nodeData.system.getHostByNode(nodeData.context.self)
    val demands: Set[Demand] = nodeData.query.demands.filter(d => d.conditions.exists(_.isInstanceOf[FrequencyCondition]))
    currentOutput = Some(0)
    if (demands.nonEmpty) {
      nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          demands.foreach(d => d.conditions.foreach {
            case fc: FrequencyCondition =>
              require(fc.ratio.timeSpan.getSeconds <= interval)
              // `divisor`, e.g., if `interval` == 30, and `fc.ratio.timeSpan.getSeconds` == 10, then `divisor` == 3
              val divisor: Int = interval / fc.ratio.timeSpan.getSeconds
              val frequency = if(testing) host.lastFrequency.instances.getInstanceNum else currentOutput.get / divisor
              if (logging) println(s"FREQUENCY:\tOn average, node `${nodeData.name}` emits $frequency events every " +
                s"${fc.ratio.timeSpan.getSeconds} seconds. (Calculated every $interval seconds.)")
              val instances = fc.ratio.instances.getInstanceNum
              fc.booleanOperator match {
                case Equal =>        fc.notFulfilled = !(frequency == instances)
                case NotEqual =>     fc.notFulfilled = !(frequency != instances)
                case Greater =>      fc.notFulfilled = !(frequency > instances)
                case GreaterEqual => fc.notFulfilled = !(frequency >= instances)
                case Smaller =>      fc.notFulfilled = !(frequency < instances)
                case SmallerEqual => fc.notFulfilled = !(frequency <= instances)
              }

          })
          currentOutput = Some(0)
        }
      )
    }
  }

  def onEventEmit(event: Event): Unit =  if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)


}
