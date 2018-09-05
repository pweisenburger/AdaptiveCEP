package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.ActorRef
import com.espertech.esper.client._
import shapeless.HList

case class ConjunctionNode(
    query: ConjunctionQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends BinaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode1, childNode2))
    case Created if sender() == childNode1 =>
      childNode1Created = true
      if (childNode2Created) emitCreated()
    case Created if sender() == childNode2 =>
      childNode2Created = true
      if (childNode1Created) emitCreated()
    case event: Event if sender() == childNode1 =>
      sendEvent("sq1", event.es.map(toAnyRef).toArray)
    case event: Event if sender() == childNode2 =>
      sendEvent("sq2", event.es.map(toAnyRef).toArray)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
  addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))

  val epStatement: EPStatement = createEpStatement("select * from pattern [every (sq1=sq1 and sq2=sq2)]")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("sq1").asInstanceOf[Array[Any]] ++
      eventBean.get("sq2").asInstanceOf[Array[Any]]
    val event = Event(values: _*)
    emitEvent(event)
  })

  epStatement.addListener(updateListener)

}
