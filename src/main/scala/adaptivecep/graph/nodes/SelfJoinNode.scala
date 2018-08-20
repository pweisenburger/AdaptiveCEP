package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.JoinNode._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.ActorRef
import com.espertech.esper.client._

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
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode =>
      sendEvent("sq", event.es.map(toAnyRef).toArray)
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
    if (values.length < 2)
      sys.error(s"values should have a length of at least 2")
    val event = Event(values: _*)
    emitEvent(event)
  })

  epStatement.addListener(updateListener)

}
