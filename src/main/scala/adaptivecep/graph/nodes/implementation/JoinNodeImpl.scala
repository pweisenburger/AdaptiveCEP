package adaptivecep.graph.nodes.implementation

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.implementation.JoinNodeImpl._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.ActorRef
import com.espertech.esper.client._

case class JoinNodeImpl(
    query: JoinQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any],
    childNode1: ActorRef,
    childNode2: ActorRef)
  extends BinaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  def this(args: Tuple8[JoinQuery, Map[String, ActorRef],
    MonitorFactory, MonitorFactory, Option[()=>Any], Option[(Event)=>Any], ActorRef, ActorRef]) = {
    this(args._1, args._2, args._3, args._4, args._5, args._6, args._7, args._8)
  }

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
    case PrepareForUpdate =>
      val fullState = FullState(nodeData, frequencyMonitor, latencyMonitor)
      sender() ! TransferState(fullState)
    case TransferState(state) =>
      nodeData = state.data.asInstanceOf[BinaryNodeData]
      frequencyMonitor = state.frequencyMonitor.asInstanceOf[BinaryNodeMonitor]
      latencyMonitor = state.latencyMonitor.asInstanceOf[BinaryNodeMonitor]

    //uncomment in case of local simulation
    //UpdateGeneratorLocal.setFinish(System.currentTimeMillis())
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

object JoinNodeImpl {

  def createWindowEplString(window: Window): String = window match {
    case SlidingInstances(instances) => s"win:length($instances)"
    case TumblingInstances(instances) => s"win:length_batch($instances)"
    case SlidingTime(seconds) => s"win:time($seconds)"
    case TumblingTime(seconds) => s"win:time_batch($seconds)"
  }

}
