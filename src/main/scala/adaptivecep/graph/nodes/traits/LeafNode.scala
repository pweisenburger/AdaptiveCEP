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

  var nodeData: LeafNodeData = LeafNodeData(name, requirements, context, parentNode)

  var resetTask: Cancellable = null

  override def preStart(): Unit = {
    if(resetTask == null){
      resetTask = context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(1000, TimeUnit.MILLISECONDS),
        runnable = () => {
          emittedEvents = 0
        })
    }
    emitCreated()
    postCreated()
  }

  def postCreated(): Unit = {
    //NO OP
  }

  def emitCreated(): Unit = {
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    emittedEvents += 1
    if (eventCallback.isDefined) eventCallback.get.apply(event) else source._1.offer(event)
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }
}
