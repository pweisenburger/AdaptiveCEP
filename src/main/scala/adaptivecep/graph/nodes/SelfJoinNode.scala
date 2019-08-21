package adaptivecep.graph.nodes

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.JoinNode._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.ConversionRules._
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}
import com.espertech.esper.client._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global

case class SelfJoinNode(
                         requirements: Set[Requirement],
                         windowType1: String,
                         windowSize1: Int,
                         windowType2: String,
                         windowSize2: Int,
                         queryLength: Int,
                         publishers: Map[String, ActorRef],
                         frequencyMonitorFactory: MonitorFactory,
                         latencyMonitorFactory: MonitorFactory,
                         createdCallback: Option[() => Any],
                         eventCallback: Option[(Event) => Any],
                         encryptedEvents: Boolean = false)
  extends UnaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  var parentReceived: Boolean = false
  var childCreated: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      childCreated = true
    case CentralizedCreated =>
      if (!created) {
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case SourceRequest =>
      source = Source.queue[Event](20000, OverflowStrategy.dropNew).preMaterialize()(materializer)
      future = source._2.runWith(StreamRefs.sourceRef())(materializer)
      sourceRef = Await.result(future, Duration.Inf)
      sender() ! SourceResponse(sourceRef)
    case SourceResponse(ref) =>
      val s = sender()
      ref.getSource.to(Sink foreach (e => {
        processEvent(e, s)
      })).run(materializer)
    case Child1(c) => {
      childNode = c
      c ! SourceRequest
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
      emitCreated()
    }
    case ChildUpdate(_, a) => {
      emitCreated()
      childNode = a
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      scheduledTask.cancel()
      lmonitor.get.scheduledTask.cancel()
    case Controller(c) =>
      controller = c

    case CostReport(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(CostReport(c), nodeData)
      latencyMonitor.onMessageReceive(CostReport(c), nodeData)
    case e: Event => processEvent(e, sender())
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  var inputRule: Option[EventConversionRule] = None
  var resultRule: Option[EventConversionRule] = None

  /**
    * we should limit the input query here to not generate more than Event6 or EncEvent6
    * the limit should be from 1 to 3
    * Self join on Event1 results in Event2
    * Self join on Event2 results in Event4
    * Self join on Event3 results in Event6
    * @param event
    * @param sender
    */
  def processEvent(event: Event, sender: ActorRef): Unit = {
    processedEvents += 1
    if (sender == childNode) {
      event match {
        case Event1(e1) => sendEvent("sq", Array(toAnyRef(e1)))
        case Event2(e1, e2) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2)))
        case Event3(e1, e2, e3) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
        case Event4(e1, e2, e3, e4) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
        case Event5(e1, e2, e3, e4, e5) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
        case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
        case EncEvent1(e1, rule) => sendEvent("sq", Array(toAnyRef(e1)))
          if (inputRule.isEmpty) inputRule = Some(rule)
        case EncEvent2(e1, e2, rule) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2)))
          if (inputRule.isEmpty) inputRule = Some(rule)
        case EncEvent3(e1, e2, e3, rule) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
          if (inputRule.isEmpty) inputRule = Some(rule)
        case EncEvent4(e1, e2, e3, e4, rule) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
          if (inputRule.isEmpty) inputRule = Some(rule)
        case EncEvent5(e1, e2, e3, e4, e5, rule) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
          if (inputRule.isEmpty) inputRule = Some(rule)
        case EncEvent6(e1, e2, e3, e4, e5, e6, rule) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
          if (inputRule.isEmpty) inputRule = Some(rule)
      }
    }
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq", createArrayOfNames(queryLength), createArrayOfClasses(queryLength))

  //addEventType("sq", createArrayOfNames(query.sq), createArrayOfClasses(query.sq))

  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
      s"sq.${createWindowEplString(createWindow(windowType1, windowSize1))} as lhs, " +
      s"sq.${createWindowEplString(createWindow(windowType2, windowSize2))} as rhs")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("lhs").asInstanceOf[Array[Any]] ++
        eventBean.get("rhs").asInstanceOf[Array[Any]]

    val event: Event = (encryptedEvents, values.length) match {
      case (false, 2) => Event2(values(0), values(1))
      case (false, 3) => Event3(values(0), values(1), values(2)) /// should be impossible for self join!
      case (false, 4) => Event4(values(0), values(1), values(2), values(3))
      case (false, 5) => Event5(values(0), values(1), values(2), values(3), values(4)) /// should be impossible for self join!
      case (false, 6) => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
      case (true, 2) => EncEvent2(values(0), values(1), getResultRule2)
//      case (true, 3) => EncEvent3(values(0), values(1), values(2), getResultRule3) /// should be impossible for self join!
      case (true, 4) => EncEvent4(values(0), values(1), values(2), values(3), getResultRule4)
//      case (true, 5) => EncEvent5(values(0), values(1), values(2), values(3), values(4), getResultRule5) /// should be impossible for self join!
      case (true, 6) => EncEvent6(values(0), values(1), values(2), values(3), values(4), values(5), getResultRule6)

    }

//    val event: Event = values.length match {
//      case 2 => Event2(values(0), values(1))
//        //** this is impossible in self join node!
//      case 3 => Event3(values(0), values(1), values(2))
//      case 4 => Event4(values(0), values(1), values(2), values(3))
//      //** this is impossible in self join node!
//      case 5 => Event5(values(0), values(1), values(2), values(3), values(4))
//      case 6 => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
//    }
    emitEvent(event)
  })

  epStatement.addListener(updateListener)



  /** *
    * checks for the possible combinations to return an event rule of type 2
    *
    * @return
    */
  private def getResultRule2: Event2Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event2Rule]
    else {
      inputRule.get match {
        case Event1Rule(e1tr1) =>
          resultRule = Some(Event2Rule(e1tr1, e1tr1))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event2Rule]
    }
  }



  private def getResultRule4: Event4Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event4Rule]
    else {
      inputRule.get match {
        case Event2Rule(e1tr1, e1tr2) =>
          resultRule = Some(Event4Rule(e1tr1, e1tr2, e1tr1, e1tr2))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event4Rule]
    }
  }


  private def getResultRule6: Event6Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event6Rule]
    else {
      inputRule.get match {
        case Event3Rule(e1tr1, e1tr2, e1tr3) =>
          resultRule = Some(Event6Rule(e1tr1, e1tr2, e1tr3, e1tr1, e1tr2, e1tr3))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event6Rule]
    }
  }


}
