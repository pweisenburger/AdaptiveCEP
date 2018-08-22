package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.publishers.Publisher._
import akka.actor.ActorRef
import com.espertech.esper.client._
import shapeless.HList

case class SequenceNode[A <: HList, B <: HList](
    query: SequenceQuery[A, B],
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends LeafNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  val queryPublishers: Array[ActorRef] = Array(publishers(query.s1.publisherName), publishers(query.s2.publisherName))

  queryPublishers.foreach(_ ! Subscribe)

  var subscription1Acknowledged: Boolean = false
  var subscription2Acknowledged: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription if sender() == queryPublishers(0) =>
      subscription1Acknowledged = true
      if (subscription2Acknowledged) emitCreated()
    case AcknowledgeSubscription if sender() == queryPublishers(1) =>
      subscription2Acknowledged = true
      if (subscription1Acknowledged) emitCreated()
    case event: Event if sender() == queryPublishers(0) =>
      sendEvent("sq1", event.es.map(toAnyRef).toArray)
    case event: Event if sender() == queryPublishers(1) =>
      sendEvent("sq2", event.es.map(toAnyRef).toArray)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq1", SequenceNode.createArrayOfNames(query.s1), SequenceNode.createArrayOfClasses(query.s1))
  addEventType("sq2", SequenceNode.createArrayOfNames(query.s2), SequenceNode.createArrayOfClasses(query.s2))

  val epStatement: EPStatement = createEpStatement("select * from pattern [every (sq1=sq1 -> sq2=sq2)]")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("sq1").asInstanceOf[Array[Any]] ++
      eventBean.get("sq2").asInstanceOf[Array[Any]]
    val event = Event(values: _*)
    emitEvent(event)
  })

  epStatement.addListener(updateListener)

}

object SequenceNode {

  def createArrayOfNames(noReqStream: NStream): Array[String] = noReqStream match {
    case hnstream: HListNStream[_] =>
      (for (i <- 1 to hnstream.length) yield "e" + i).toArray
    case _ => throw new IllegalArgumentException("NStream has only one subclass")
  }

  def createArrayOfClasses(noReqStream: NStream): Array[Class[_]] = noReqStream match {
    case hnstream: HListNStream[_] =>
      val clazz: Class[_] = classOf[AnyRef]
      (for (i <- 1 to hnstream.length) yield clazz).toArray
    case _ => throw new IllegalArgumentException("NStream has only one subclass")
  }

}
