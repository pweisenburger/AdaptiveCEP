package adaptivecep.graph.qos

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.{ActorContext, Cancellable}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._

trait AverageFrequencyMonitor {

  val logging: Boolean
  val interval: Int

  var currentOutput: Option[Int] = None
  var averageOutput: Option[Int] = None
  var violatedRequirements: Set[Requirement] = Set.empty[Requirement]
  var scheduledTask: Cancellable = null

  def onCreated(name: String, requirements: Set[Requirement], context: ActorContext): Unit = {
    val frequencyRequirements: Set[FrequencyRequirement] = requirements.collect{ case fr: FrequencyRequirement => fr }
    val callbackNodeData: NodeData = NodeData(name, requirements, context)
    currentOutput = Some(0)
    if (frequencyRequirements.nonEmpty) {
      if(scheduledTask == null){
        scheduledTask = context.system.scheduler.schedule(
          initialDelay = FiniteDuration(0, TimeUnit.MILLISECONDS),
          interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
          runnable = () => {
            frequencyRequirements.foreach(requirement => {
              require(requirement.seconds <= interval)
              // `divisor`, e.g., if `interval` == 30, and `requirement.seconds` == 10, then `divisor` == 3
              val divisor: Int = interval / (requirement.seconds * 1000)
              val frequency: Int = currentOutput.get / divisor
              averageOutput = Some(frequency)
              //println(currentOutput.get, divisor, frequency)
              if (logging) println(
                s"FREQUENCY:\tOn average, node `$name` emits $frequency events every ${requirement.seconds} seconds. " +
                  s"(Calculated every $interval seconds.)")
              requirement.operator match {
                case Equal =>        if (!(frequency == requirement.instances)){
                  violatedRequirements += requirement
                  requirement.callback(callbackNodeData)
                } else {
                  violatedRequirements -= requirement
                }
                case NotEqual =>     if (frequency == requirement.instances){
                  violatedRequirements += requirement
                  requirement.callback(callbackNodeData)
                } else {
                  violatedRequirements -= requirement
                }
                case Greater =>      if (!(frequency >  requirement.instances)){
                  violatedRequirements += requirement
                  requirement.callback(callbackNodeData)
                } else {
                  violatedRequirements -= requirement
                }
                case GreaterEqual => if (!(frequency >= requirement.instances)){
                  violatedRequirements += requirement
                  requirement.callback(callbackNodeData)
                } else {
                  violatedRequirements -= requirement
                }
                case Smaller =>      if (!(frequency <  requirement.instances)){
                  violatedRequirements += requirement
                  requirement.callback(callbackNodeData)
                } else {
                  violatedRequirements -= requirement
                }
                case SmallerEqual => if (!(frequency <= requirement.instances)){
                  violatedRequirements += requirement
                  requirement.callback(callbackNodeData)
                } else {
                  violatedRequirements -= requirement
                }
              }
            })
            currentOutput = Some(0)
          }
        )
      }
    }
    else{
      if(scheduledTask == null){
        scheduledTask = context.system.scheduler.schedule(
          initialDelay = FiniteDuration(0, TimeUnit.MILLISECONDS),
          interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
          runnable = () => {
            val divisor: Int = interval / (1 * 1000)
            val frequency: Int = currentOutput.get / divisor
            averageOutput = Some(frequency)
            currentOutput = Some(0)
          })
      }
    }
  }

  def onEventEmit(event: Event): Unit = {
    //println(currentOutput)
    if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)
  }
}

case class AverageFrequencyLeafNodeMonitor(interval: Int, logging: Boolean) extends AverageFrequencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated(nodeData.name, nodeData.requirements, nodeData.context)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyUnaryNodeMonitor(interval: Int, logging: Boolean) extends AverageFrequencyMonitor with UnaryNodeMonitor {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated(nodeData.name, nodeData.requirements, nodeData.context)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyBinaryNodeMonitor(interval: Int, logging: Boolean) extends AverageFrequencyMonitor with BinaryNodeMonitor {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated(nodeData.name, nodeData.requirements, nodeData.context)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit(event)

}

case class AverageFrequencyMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = AverageFrequencyLeafNodeMonitor(interval, logging)
  override def createUnaryNodeMonitor: UnaryNodeMonitor = AverageFrequencyUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = AverageFrequencyBinaryNodeMonitor(interval, logging)

}
