package adaptivecep.graph.nodes.implementation

import adaptivecep.data.Events._
import adaptivecep.data.Queries.{ConjunctionQuery, SelfJoinQuery}
import JoinNodeImpl._
import adaptivecep.graph.nodes.traits.EsperEngine.{createArrayOfClasses, createArrayOfNames, toAnyRef}
import adaptivecep.graph.nodes.traits.{EsperEngine, UnaryNode}
import adaptivecep.graph.qos.{BinaryNodeData, FullState, LeafNodeData, LeafNodeMonitor, MonitorFactory, UnaryNodeData, UnaryNodeMonitor}
import akka.actor.ActorRef
import com.espertech.esper.client.{EPStatement, EventBean, UpdateListener}

case class SelfJoinNodeImpl(
    query: SelfJoinQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any],
    childNode: ActorRef)
  extends UnaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  def this(args: Tuple7[SelfJoinQuery, Map[String, ActorRef],
    MonitorFactory, MonitorFactory, Option[()=>Any], Option[(Event)=>Any], ActorRef]) = {
    this(args._1, args._2, args._3, args._4, args._5, args._6, args._7)
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
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
    case PrepareForUpdate =>
      val fullState = FullState(nodeData, frequencyMonitor, latencyMonitor)
      sender() ! TransferState(fullState)
    case TransferState(state) =>
      nodeData = state.data.asInstanceOf[UnaryNodeData]
      frequencyMonitor = state.frequencyMonitor.asInstanceOf[UnaryNodeMonitor]
      latencyMonitor = state.latencyMonitor.asInstanceOf[UnaryNodeMonitor]

    //uncomment in case of local simulation
    //UpdateGeneratorLocal.setFinish(System.currentTimeMillis())
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