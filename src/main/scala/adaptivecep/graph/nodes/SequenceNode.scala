package adaptivecep.graph.nodes

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.publishers.Publisher._
import akka.actor.{ActorRef, PoisonPill}
import com.espertech.esper.client._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

case class SequenceNode(
    //query: SequenceQuery,
    requirements: Set[Requirement],
    publisherName1: String,
    publisherName2: String,
    queryLength1: Int,
    queryLength2: Int,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    bandwidthMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends LeafNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  val queryPublishers: Array[ActorRef] = Array(publishers(publisherName1), publishers(publisherName2))
  //val queryPublishers: Array[ActorRef] = Array(publishers(query.s1.publisherName), publishers(query.s2.publisherName))

  queryPublishers.foreach(_ ! Subscribe)

  var subscription1Acknowledged: Boolean = false
  var subscription2Acknowledged: Boolean = false
  var parentReceived: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription if sender() == queryPublishers(0) =>
      subscription1Acknowledged = true
      println("Acknowledged Subscription 1")
      //if (subscription2Acknowledged && parentReceived && !created) emitCreated()
    case AcknowledgeSubscription if sender() == queryPublishers(1) =>
      subscription2Acknowledged = true
      println("Acknowledged Subscription 2")
      //if (subscription1Acknowledged && parentReceived && !created) emitCreated()
    case event: Event if sender() == queryPublishers(0) =>
      context.system.scheduler.scheduleOnce(
        FiniteDuration(costs(parentNode).duration.toMillis, TimeUnit.MILLISECONDS),
        () => {
          if(parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
            frequencyMonitor.onEventEmit(event, nodeData)
            emittedEvents += 1
            event match {
              case Event1(e1) => sendEvent("sq1", Array(toAnyRef(e1)))
              case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
              case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
              case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
              case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
              case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }}})
    case event: Event if sender() == queryPublishers(1) =>
      context.system.scheduler.scheduleOnce(
        FiniteDuration(costs(parentNode).duration.toMillis, TimeUnit.MILLISECONDS),
        () => {
          if(parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
            frequencyMonitor.onEventEmit(event, nodeData)
            emittedEvents += 1
            event match {
              case Event1(e1) => sendEvent("sq2", Array(toAnyRef(e1)))
              case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
              case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
              case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
              case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
              case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }}})
    case CentralizedCreated =>
      if(!created){
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      //println("Parent received", p1)
      parentNode = p1
      parentReceived = true
      nodeData = LeafNodeData(name, requirements, context, parentNode)
      //if (subscription1Acknowledged && subscription2Acknowledged) emitCreated()
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      //self ! PoisonPill
      //fMonitor.scheduledTask.cancel()
      //println("Shutting down....")
    case Controller(c) =>
      controller = c
      //println("Got Controller", c)
    case CostReport(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(CostReport(c), nodeData)
      latencyMonitor.onMessageReceive(CostReport(c), nodeData)
      bandwidthMonitor.onMessageReceive(CostReport(c), nodeData)
    case _: Event =>
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      bandwidthMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }
  addEventType("sq1", createArrayOfNames(queryLength1), createArrayOfClasses(queryLength1))
  addEventType("sq2", createArrayOfNames(queryLength2), createArrayOfClasses(queryLength2))
/*
  addEventType("sq1", SequenceNode.createArrayOfNames(query.s1), SequenceNode.createArrayOfClasses(query.s1))
  addEventType("sq2", SequenceNode.createArrayOfNames(query.s2), SequenceNode.createArrayOfClasses(query.s2))
*/
  val epStatement: EPStatement = createEpStatement("select * from pattern [every (sq1=sq1 -> sq2=sq2)]")

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

object SequenceNode {

  def createArrayOfNames(noReqStream: NStream): Array[String] = noReqStream match {
    case _: NStream1[_] => Array("e1")
    case _: NStream2[_, _] => Array("e1", "e2")
    case _: NStream3[_, _, _] => Array("e1", "e2", "e3")
    case _: NStream4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: NStream5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
  }

  def createArrayOfClasses(noReqStream: NStream): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    noReqStream match {
      case _: NStream1[_] => Array(clazz)
      case _: NStream2[_, _] => Array(clazz, clazz)
      case _: NStream3[_, _, _] => Array(clazz, clazz, clazz)
      case _: NStream4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: NStream5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
    }
  }

  def createArrayOfNames(length: Int): Array[String] = length match {
    case 1 => Array("e1")
    case 2 => Array("e1", "e2")
    case 3 => Array("e1", "e2", "e3")
    case 4 => Array("e1", "e2", "e3", "e4")
    case 5 => Array("e1", "e2", "e3", "e4", "e5")
  }

  def createArrayOfClasses(length: Int): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    length match {
      case 1 => Array(clazz)
      case 2 => Array(clazz, clazz)
      case 3 => Array(clazz, clazz, clazz)
      case 4 => Array(clazz, clazz, clazz, clazz)
      case 5 => Array(clazz, clazz, clazz, clazz, clazz)
    }
  }

}
