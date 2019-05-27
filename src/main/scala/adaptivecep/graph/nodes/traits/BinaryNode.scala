package adaptivecep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, Cancellable}
import akka.stream.SourceRef

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}

trait BinaryNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  val query: Query1[Int] = stream[Int]("A")

  val interval = 10
  var childNode1: ActorRef = self
  var childNode2: ActorRef = self
  var childSourceRef1: SourceRef[Event] = null
  var childSourceRef2: SourceRef[Event] = null

  var parentNode: ActorRef = self

  var frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  var latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  //var bandwidthMonitor: BinaryNodeMonitor = bandwidthMonitorFactory.createBinaryNodeMonitor
  var nodeData: BinaryNodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
  var scheduledTask: Cancellable = _
  var resetTask: Cancellable = null

  val lmonitor: Option[PathLatencyBinaryNodeMonitor] = latencyMonitor match {
    case m: PathLatencyBinaryNodeMonitor => Some(m)
    case _ => None
  }
  //val bmonitor: PathBandwidthUnaryNodeMonitor = bandwidthMonitor.asInstanceOf[PathBandwidthUnaryNodeMonitor]
  var fMonitor: Option[AverageFrequencyBinaryNodeMonitor] = frequencyMonitor match {
    case m :AverageFrequencyBinaryNodeMonitor => Some(m)
    case _ => None
  }

  var previousBandwidth : Double = 0
  var previousLatency : Duration = Duration.Zero

  override def preStart(): Unit = {
    if(resetTask == null){
      resetTask = context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(1000, TimeUnit.MILLISECONDS),
        runnable = () => {
          //println("emitted " + emittedEvents)
          //println("processed " + processedEvents)
          emittedEvents = 0
          processedEvents = 0
        })
    }
    emitCreated()
    if(scheduledTask == null && lmonitor.isDefined && fMonitor.isDefined){
      scheduledTask = context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          if (lmonitor.get.latency.isDefined && fMonitor.get.averageOutput.isDefined /*bmonitor.bandwidthForMonitoring.isDefined*/ ) {
            if (lmonitor.get.violatedRequirements.nonEmpty || fMonitor.get.violatedRequirements.nonEmpty) {
              controller ! RequirementsNotMet(lmonitor.get.violatedRequirements.++(fMonitor.get.violatedRequirements))
              lmonitor.get.violatedRequirements = Set.empty[Requirement]
              //bmonitor.met = true
              fMonitor.get.violatedRequirements = Set.empty[Requirement]
            }

            if (fMonitor.get.violatedRequirements.isEmpty && fMonitor.get.violatedRequirements.isEmpty) {
              controller ! RequirementsMet
            }
            println(lmonitor.get.latency.get.toNanos / 1000000.0 + ", " + fMonitor.get.averageOutput.get /*bmonitor.bandwidthForMonitoring.get*/)
            //println(costs(parentNode))
            previousLatency = FiniteDuration(lmonitor.get.latency.get.toMillis, TimeUnit.MILLISECONDS)
            previousBandwidth = fMonitor.get.averageOutput.get
            lmonitor.get.latency = None
            fMonitor.get.averageOutput = None
            //bmonitor.bandwidthForMonitoring = None
          } else {
            println(previousLatency.toNanos/1000000.0 + ", " + previousBandwidth)
          }
        }
      )
    }
  }

  override def postStop(): Unit = {
    scheduledTask.cancel()
    if(lmonitor.isDefined) lmonitor.get.scheduledTask.cancel()
    //bmonitor.scheduledTask.cancel()
    //println("Shutting down....")
  }

  def emitCreated(): Unit = {
    if(lmonitor.isDefined){
      lmonitor.get.childNode1 = childNode1
      lmonitor.get.childNode2 = childNode2
    }
    //bmonitor.childNode1 = childNode1
    //bmonitor.childNode2 = childNode2
    //if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
    //bandwidthMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if(lmonitor.isDefined){
      lmonitor.get.childNode1 = childNode1
      lmonitor.get.childNode2 = childNode2
    }
    if(parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
      emittedEvents += 1
      //println(event)
      //bmonitor.childNode1 = childNode1
      //bmonitor.childNode2 = childNode2
      if (eventCallback.isDefined) eventCallback.get.apply(event) else source._1.offer(event)//parentNode ! event
      frequencyMonitor.onEventEmit(event, nodeData)
      latencyMonitor.onEventEmit(event, nodeData)
      //bandwidthMonitor.onEventEmit(event, nodeData)
    }
  }
}