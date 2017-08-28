package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.Host
import com.lambdarookie.eventscala.data.Events._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

case class AverageFrequencyMonitor(interval: Int, logging: Boolean, counting: Boolean) extends Monitor {
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
              require(fc.ratio.timeSpan.toSeconds <= interval)
              // `divisor`, e.g., if `interval` == 30, and `fc.ratio.timeSpan.toSeconds` == 10, then `divisor` == 3
              val divisor: Int = interval / fc.ratio.timeSpan.toSeconds
              val current: Int = if(counting)
                currentOutput.get / divisor
              else
                host.measureFrequency().instances.getInstanceNum
              if (logging) println(s"FREQUENCY:\tOn average, node `${nodeData.name}` emits $current events every " +
                s"${fc.ratio.timeSpan.toSeconds} seconds. (Calculated every $interval seconds.)")
              val expected = fc.ratio.instances.getInstanceNum
              fc.booleanOperator match {
                case Equal =>        fc.notFulfilled = !(current == expected)
                case NotEqual =>     fc.notFulfilled = !(current != expected)
                case Greater =>      fc.notFulfilled = !(current > expected)
                case GreaterEqual => fc.notFulfilled = !(current >= expected)
                case Smaller =>      fc.notFulfilled = !(current < expected)
                case SmallerEqual => fc.notFulfilled = !(current <= expected)
              }
            case _ => //Do nothing
          })
          currentOutput = Some(0)
        }
      )
    }
  }

  override def copy: AverageFrequencyMonitor = AverageFrequencyMonitor(interval, logging, counting)

  def onEventEmit(event: Event): Unit =  if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)


}
