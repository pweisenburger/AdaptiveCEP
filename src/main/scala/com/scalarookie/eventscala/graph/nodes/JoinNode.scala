package com.scalarookie.eventscala.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import com.scalarookie.eventscala.data.Events._
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph.nodes.traits._
import com.scalarookie.eventscala.graph.qos._
import JoinNode._

object JoinNode {

  def createArrayOfNames(query: Query): Array[String] = query match {
    case _: Query1[_] => Array("e1")
    case _: Query2[_, _] => Array("e1", "e2")
    case _: Query3[_, _, _] => Array("e1", "e2", "e3")
    case _: Query4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: Query5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
    case _: Query6[_, _, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5", "e6")
  }

  def createArrayOfClasses(query: Query): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    query match {
      case _: Query1[_] => Array(clazz)
      case _: Query2[_, _] => Array(clazz, clazz)
      case _: Query3[_, _, _] => Array(clazz, clazz, clazz)
      case _: Query4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: Query5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
      case _: Query6[_, _, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz, clazz)
    }
  }

  def toAnyRef(any: Any): AnyRef = {
    // Yep, you can do that!
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

  def createWindowEplString(window: Window): String = window match {
    case SlidingInstances(instances) => s"win:length($instances)"
    case TumblingInstances(instances) => s"win:length_batch($instances)"
    case SlidingTime(seconds) => s"win:time($seconds)"
    case TumblingTime(seconds) => s"win:time_batch($seconds)"
  }

}

case class JoinNode(
    query: JoinQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends BinaryNode with EsperEngine {

  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false

  override val esperServiceProviderUri: String = name

  override def receive: Receive = {
    case Created if sender() == childNode1 =>
      childNode1Created = true
      if (childNode2Created) emitCreated()
    case Created if sender() == childNode2 =>
      childNode2Created = true
      if (childNode1Created) emitCreated()
    case event: Event if sender() == childNode1 => event match {
      case Event1(e1) => sendEvent("sq1", Array(toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case event: Event if sender() == childNode2 => event match {
      case Event1(e1) => sendEvent("sq2", Array(toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
  addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))

  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
    s"sq1.${createWindowEplString(query.w1)} as sq1, " +
    s"sq2.${createWindowEplString(query.w2)} as sq2")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("sq1").asInstanceOf[Array[Any]] ++
      eventBean.get("sq2").asInstanceOf[Array[Any]]
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
