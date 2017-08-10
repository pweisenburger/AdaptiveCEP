package com.lambdarookie.eventscala.graph.monitors

import java.util.concurrent.TimeUnit

import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits.Host
import com.lambdarookie.eventscala.data.Events._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

trait AverageFrequencyMonitor {

  val interval: Int
  val logging: Boolean
  val testing: Boolean

  var currentOutput: Option[Int] = None

  def onCreated[ND <: NodeData](nodeData: ND): Unit = {
    val host: Host = nodeData.system.getHostByNode(nodeData.context.self)
    val demands: Set[Demand] = nodeData.query.demands.filter(d => d.conditions.exists(_.isInstanceOf[FrequencyCondition]))
    currentOutput = Some(0)
    if (demands.nonEmpty) {
      nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          nodeData.system.measureFrequencies()
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

  def onEventEmit(event: Event): Unit = {
    if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)
  }

}

case class AverageFrequencyLeafNodeMonitor(interval: Int, logging: Boolean, testing: Boolean)
  extends AverageFrequencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated[LeafNodeData](nodeData)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyUnaryNodeMonitor(interval: Int, logging: Boolean, testing: Boolean)
  extends AverageFrequencyMonitor with UnaryNodeMonitor {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated[UnaryNodeData](nodeData)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyBinaryNodeMonitor(interval: Int, logging: Boolean, testing: Boolean)
  extends AverageFrequencyMonitor with BinaryNodeMonitor {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated[BinaryNodeData](nodeData)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyMonitorFactory(interval: Int, logging: Boolean, testing: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor =
    AverageFrequencyLeafNodeMonitor(interval, logging, testing: Boolean)
  override def createUnaryNodeMonitor: UnaryNodeMonitor =
    AverageFrequencyUnaryNodeMonitor(interval, logging, testing: Boolean)
  override def createBinaryNodeMonitor: BinaryNodeMonitor =
    AverageFrequencyBinaryNodeMonitor(interval, logging, testing: Boolean)

}
