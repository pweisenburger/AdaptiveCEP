package adaptivecep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries.Query1
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, Cancellable}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

trait LeafNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  var parentNode: ActorRef = self

  val query: Query1[Int] = stream[Int]("A")

  var frequencyMonitor: LeafNodeMonitor = frequencyMonitorFactory.createLeafNodeMonitor
  var latencyMonitor: LeafNodeMonitor = latencyMonitorFactory.createLeafNodeMonitor
  var bandwidthMonitor: LeafNodeMonitor = bandwidthMonitorFactory.createLeafNodeMonitor
  var nodeData: LeafNodeData = LeafNodeData(name, requirements, context, parentNode)

  var fMonitor: AverageFrequencyLeafNodeMonitor = frequencyMonitor.asInstanceOf[AverageFrequencyLeafNodeMonitor]
  var resetTask: Cancellable = null

  private val monitor: PathLatencyLeafNodeMonitor = latencyMonitor.asInstanceOf[PathLatencyLeafNodeMonitor]

  override def preStart(): Unit = {
    if(resetTask == null){
      resetTask = context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(1000, TimeUnit.MILLISECONDS),
        runnable = () => {
          //println("emitted " + emittedEvents)
          emittedEvents = 0
        })
    }
    emitCreated()
  }


  def emitCreated(): Unit = {
    if (createdCallback.isDefined){createdCallback.get.apply()} //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
    bandwidthMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if(parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
      emittedEvents += 1
      //println("sending to " + parentNode)
      if (eventCallback.isDefined) eventCallback.get.apply(event) else source._1.offer(event)//parentNode ! event
      frequencyMonitor.onEventEmit(event, nodeData)
      latencyMonitor.onEventEmit(event, nodeData)
      bandwidthMonitor.onEventEmit(event, nodeData)
    }
  }
}
