package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.qos._

case class FilterNode(
    query: FilterQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends Node {

  val childNode: ActorRef = createChildNode(1, query.sq)

  val nodeData: UnaryNodeData = UnaryNodeData(name, query, context, childNode)

  val frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  val latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor

  def emitCreated(): Unit = {
    if (createdCallback.isDefined) createdCallback.get.apply() else context.parent ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (eventCallback.isDefined) eventCallback.get.apply(event) else context.parent ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

  def handleEvent1(e1: Any): Unit =
    if (query.asInstanceOf[KeepEventsWith1[Any]].cond(e1)) emitEvent(Event1(e1))

  def handleEvent2(e1: Any, e2: Any): Unit =
    if (query.asInstanceOf[KeepEventsWith2[Any, Any]].cond(e1, e2)) emitEvent(Event2(e1, e2))

  def handleEvent3(e1: Any, e2: Any, e3: Any): Unit =
    if (query.asInstanceOf[KeepEventsWith3[Any, Any, Any]].cond(e1, e2, e3)) emitEvent(Event3(e1, e2, e3))

  def handleEvent4(e1: Any, e2: Any, e3: Any, e4: Any): Unit =
    if (query.asInstanceOf[KeepEventsWith4[Any, Any, Any, Any]].cond(e1, e2, e3, e4)) emitEvent(Event4(e1, e2, e3, e4))

  def handleEvent5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any): Unit =
    if (query.asInstanceOf[KeepEventsWith5[Any, Any, Any, Any, Any]].cond(e1, e2, e3, e4, e5)) emitEvent(Event5(e1, e2, e3, e4, e5))

  def handleEvent6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any): Unit =
    if (query.asInstanceOf[KeepEventsWith6[Any, Any, Any, Any, Any, Any]].cond(e1, e2, e3, e4, e5, e6)) emitEvent(Event6(e1, e2, e3, e4, e5, e6))

  override def receive: Receive = {
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode => event match {
      case Event1(e1) => handleEvent1(e1)
      case Event2(e1, e2) => handleEvent2(e1, e2)
      case Event3(e1, e2, e3) => handleEvent3(e1, e2, e3)
      case Event4(e1, e2, e3, e4) => handleEvent4(e1, e2, e3, e4)
      case Event5(e1, e2, e3, e4, e5) => handleEvent5(e1, e2, e3, e4, e5)
      case Event6(e1, e2, e3, e4, e5, e6) => handleEvent6(e1, e2, e3, e4, e5, e6)
    }
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
