package adaptivecep.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.qos._
import JoinNode._

case class SelfJoinNode(
    query: SelfJoinQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node with EsperEngine {

  val childNode: ActorRef = createChildNode(1, query.sq)

  val nodeData: UnaryNodeData = UnaryNodeData(name, query, context, childNode)

  val frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  val latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor
  //val frequencyReqs: Set[FrequencyRequirement] = query.requirements collect { case fr: FrequencyRequirement => fr }
  //val latencyReqs: Set[LatencyRequirement] = query.requirements collect { case lr: LatencyRequirement => lr }

  override val esperServiceProviderUri: String = name

  def emitGraphCreated(): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case GraphCreated if sender() == childNode =>
      emitGraphCreated()
    case event: Event if sender() == childNode => event match {
      case Event1(e1) => sendEvent("sq", Array(castToAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq", Array(castToAnyRef(e1), castToAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4), castToAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4), castToAnyRef(e5), castToAnyRef(e6)))
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
