package com.lambdarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.nodes.traits._
import com.lambdarookie.eventscala.graph.nodes.traits.EsperEngine._
import com.lambdarookie.eventscala.graph.qos._
import JoinNode._

case class SelfJoinNode(
    query: SelfJoinQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends UnaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  override def receive: Receive = {
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode => event match {
      case Event1(e1) => sendEvent("sq", Array(toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq", createArrayOfNames(query.sq), createArrayOfClasses(query.sq))

  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
    s"sq.${createWindowEplString(query.w1)} as lhs, " +
    s"sq.${createWindowEplString(query.w2)} as rhs")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("lhs").asInstanceOf[Array[Any]] ++
      eventBean.get("rhs").asInstanceOf[Array[Any]]
    val event: Event = values.length match {
      case 2 => Event2(values(0), values(1))
      case 3 => Event3(values(0), values(1), values(2))
      case 4 => Event4(values(0), values(1), values(2), values(3))
      case 5 => Event5(values(0), values(1), values(2), values(3), values(4))
      case 6 => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
    }
    emitEvent(event)
  })

  epStatement.addListener(updateListener)

}
