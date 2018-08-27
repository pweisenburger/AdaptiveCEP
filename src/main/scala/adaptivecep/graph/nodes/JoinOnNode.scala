package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.JoinNode._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.ActorRef
import com.espertech.esper.client._

case class JoinOnNode(
    query: JoinOnQuery,
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

  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
      s"sq1.${createWindowEplString(query.w1)} as sq1, " +
      s"sq2.${createWindowEplString(query.w2)} as sq2")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val es1 = eventBean.get("sq1").asInstanceOf[Array[Any]]
    val es2 = eventBean.get("sq2").asInstanceOf[Array[Any]]
    handleEvent(es1, es2)
  })

  def handleEvent(es1: Seq[Any], es2: Seq[Any]): Unit = query match {
    case jo@JoinOn(_, _, _, _, _, _, _) =>
      if (es1(jo.positionOn1) == es2(jo.positionOn2)) {
        val altered = es1 ++ es2.patch(jo.positionOn2, Nil, 1)
        emitEvent(Event(altered: _*))
      }
    case jo@JoinOnRecord(_, _, _, _, _, _, _) =>
      if (jo.selectFrom1(es1) == jo.selectFrom2(es2)) {
        val alteredEs2 = jo.drop(es2) match {
          case Some(vals) => vals
          case None => sys.error(errorMsg)
        }
        emitEvent(Event(es1 ++ alteredEs2: _*))
      }
  }

  epStatement.addListener(updateListener)

}
