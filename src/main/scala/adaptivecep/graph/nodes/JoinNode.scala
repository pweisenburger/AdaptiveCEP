package adaptivecep.graph.nodes

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.JoinNode._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.ConversionRules._
import adaptivecep.privacy.Privacy._
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}
import com.espertech.esper.client._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global

case class JoinNode(
                     requirements: Set[Requirement],
                     windowType1: String,
                     windowSize1: Int,
                     windowType2: String,
                     windowSize2: Int,
                     queryLength1: Int,
                     queryLength2: Int,
                     publishers: Map[String, ActorRef],
                     frequencyMonitorFactory: MonitorFactory,
                     latencyMonitorFactory: MonitorFactory,
                     createdCallback: Option[() => Any],
                     eventCallback: Option[(Event) => Any],
                     encryptedEvents: Boolean = false
                   ) (implicit val privacyContext: PrivacyContext = NoPrivacyContext)
  extends BinaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false
  var parentReceived: Boolean = false

  def moveTo(a: ActorRef): Unit = {
    a ! Parent(parentNode)
    a ! Child2(childNode1, childNode2)
    childNode1 ! Parent(a)
    childNode2 ! Parent(a)
    parentNode ! ChildUpdate(self, a)
    childNode1 ! KillMe
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode1, childNode2))
    case Created if sender() == childNode1 =>
      childNode1Created = true
    case Created if sender() == childNode2 =>
      childNode2Created = true
    case CentralizedCreated =>
      if (!created) {
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
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
    case Child2(c1, c2) => {
      childNode1 = c1
      childNode2 = c2
      c1 ! SourceRequest
      c2 ! SourceRequest
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
      emitCreated()
    }
    case ChildUpdate(old, a) => {
      emitCreated()
      if (childNode1.eq(old)) {
        childNode1 = a
      }
      if (childNode2.eq(old)) {
        childNode2 = a
      }
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      scheduledTask.cancel()
      if (lmonitor.isDefined) lmonitor.get.scheduledTask.cancel()
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

  var rule1: Option[EventConversionRule] = None
  var rule2: Option[EventConversionRule] = None
  var resultRule: Option[EventConversionRule] = None

  def processEvent(event: Event, sender: ActorRef): Unit = {
    //println(event)
    processedEvents += 1
    if (sender == childNode1) {
      event match {
        case Event1(e1) => sendEvent("sq1", Array(toAnyRef(e1)))
        case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
        case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
        case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
        case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
        case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
        case EncEvent1(e1, rule) =>
          if (rule1.isEmpty) rule1 = Some(rule)
          sendEvent("sq1", Array(toAnyRef(e1)))
        case EncEvent2(e1, e2, rule) =>
          if (rule1.isEmpty) rule1 = Some(rule)
          sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
        case EncEvent3(e1, e2, e3, rule) =>
          if (rule1.isEmpty) rule1 = Some(rule)
          sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
        case EncEvent4(e1, e2, e3, e4, rule) =>
          if (rule1.isEmpty) rule1 = Some(rule)
          sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
        case EncEvent5(e1, e2, e3, e4, e5, rule) =>
          if (rule1.isEmpty) rule1 = Some(rule)
          sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
        case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
          if (rule1.isEmpty) rule1 = Some(rule)
          sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
      }
    }

    else if (sender == childNode2) {

      event match {
        case Event1(e1) => sendEvent("sq2", Array(toAnyRef(e1)))
        case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
        case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
        case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
        case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
        case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
        case EncEvent1(e1, rule) =>
          if (rule2.isEmpty) rule2 = Some(rule)
          sendEvent("sq2", Array(toAnyRef(e1)))
        case EncEvent2(e1, e2, rule) =>
          if (rule2.isEmpty) rule2 = Some(rule)
          sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
        case EncEvent3(e1, e2, e3, rule) =>
          if (rule2.isEmpty) rule2 = Some(rule)
          sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
        case EncEvent4(e1, e2, e3, e4, rule) =>
          if (rule2.isEmpty) rule2 = Some(rule)
          sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
        case EncEvent5(e1, e2, e3, e4, e5, rule) =>
          if (rule2.isEmpty) rule2 = Some(rule)
          sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
        case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
          if (rule2.isEmpty) rule2 = Some(rule)
          sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
      }
    }
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq1", createArrayOfNames(queryLength1), createArrayOfClasses(queryLength1))
  addEventType("sq2", createArrayOfNames(queryLength2), createArrayOfClasses(queryLength2))


  /*
  addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
  addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))
  */


  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
      s"sq1.${createWindowEplString(createWindow(windowType1, windowSize1))} as sq1, " +
      s"sq2.${createWindowEplString(createWindow(windowType2, windowSize2))} as sq2")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("sq1").asInstanceOf[Array[Any]] ++
        eventBean.get("sq2").asInstanceOf[Array[Any]]
    val event: Event = (encryptedEvents, values.length) match {
      case (false, 2) => Event2(values(0), values(1))
      case (false, 3) => Event3(values(0), values(1), values(2))
      case (false, 4) => Event4(values(0), values(1), values(2), values(3))
      case (false, 5) => Event5(values(0), values(1), values(2), values(3), values(4))
      case (false, 6) => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
      case (true, 2) => EncEvent2(values(0), values(1), getResultRule2)
      case (true, 3) => EncEvent3(values(0), values(1), values(2), getResultRule3)
      case (true, 4) => EncEvent4(values(0), values(1), values(2), values(3), getResultRule4)
      case (true, 5) => EncEvent5(values(0), values(1), values(2), values(3), values(4), getResultRule5)
      case (true, 6) => EncEvent6(values(0), values(1), values(2), values(3), values(4), values(5), getResultRule6)
    }
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
      (rule1.get, rule2.get) match {
        case (Event1Rule(e1tr1), Event1Rule(e2tr1)) =>
          resultRule = Some(Event2Rule(e1tr1, e2tr1))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event2Rule]
    }
  }

  /** *
    * checks for the possible combinations to return an event rule of type 3
    * calculates the result only once
    *
    * @return
    */
  private def getResultRule3: Event3Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event3Rule]
    else {
      (rule1.get, rule2.get) match {
        case (Event1Rule(e1tr1), Event2Rule(e2tr1, e2tr2)) =>
          resultRule = Some(Event3Rule(e1tr1, e2tr1, e2tr2))
        case (Event2Rule(e1tr1, e1tr2), Event1Rule(e2tr1)) =>
          resultRule = Some(Event3Rule(e1tr1, e1tr2, e2tr1))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event3Rule]
    }
  }

  private def getResultRule4: Event4Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event4Rule]
    else {
      (rule1.get, rule2.get) match {
        case (Event1Rule(e1tr1), Event3Rule(e2tr1, e2tr2, e2tr3)) =>
          resultRule = Some(Event4Rule(e1tr1, e2tr1, e2tr2, e2tr3))
        case (Event2Rule(e1tr1, e1tr2), Event2Rule(e2tr1, e2tr2)) =>
          resultRule = Some(Event4Rule(e1tr1, e1tr2, e2tr1, e2tr2))
        case (Event3Rule(e1tr1, e1tr2, e1tr3), Event1Rule(e2tr1)) =>
          resultRule = Some(Event4Rule(e1tr1, e1tr2, e1tr3, e2tr1))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event4Rule]
    }
  }

  private def getResultRule5: Event5Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event5Rule]
    else {
      (rule1.get, rule2.get) match {
        case (Event1Rule(e1tr1), Event4Rule(e2tr1, e2tr2, e2tr3, e2tr4)) =>
          resultRule = Some(Event5Rule(e1tr1, e2tr1, e2tr2, e2tr3, e2tr4))
        case (Event2Rule(e1tr1, e1tr2), Event3Rule(e2tr1, e2tr2, e2tr3)) =>
          resultRule = Some(Event5Rule(e1tr1, e1tr2, e2tr1, e2tr2, e2tr3))
        case (Event3Rule(e1tr1, e1tr2, e1tr3), Event2Rule(e2tr1, e2tr2)) =>
          resultRule = Some(Event5Rule(e1tr1, e1tr2, e1tr3, e2tr1, e2tr2))
        case (Event4Rule(e1tr1, e1tr2, e1tr3, e1tr4), Event1Rule(e2tr1)) =>
          resultRule = Some(Event5Rule(e1tr1, e1tr2, e1tr3, e1tr4, e2tr1))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event5Rule]
    }
  }

  private def getResultRule6: Event6Rule = {
    if (resultRule.nonEmpty) resultRule.get.asInstanceOf[Event6Rule]
    else {
      (rule1.get, rule2.get) match {
        case (Event1Rule(e1tr1), Event5Rule(e2tr1, e2tr2, e2tr3, e2tr4, e2tr5)) =>
          resultRule = Some(Event6Rule(e1tr1, e2tr1, e2tr2, e2tr3, e2tr4, e2tr5))
        case (Event2Rule(e1tr1, e1tr2), Event4Rule(e2tr1, e2tr2, e2tr3, e2tr4)) =>
          resultRule = Some(Event6Rule(e1tr1, e1tr2, e2tr1, e2tr2, e2tr3, e2tr4))
        case (Event3Rule(e1tr1, e1tr2, e1tr3), Event3Rule(e2tr1, e2tr2, e2tr3)) =>
          resultRule = Some(Event6Rule(e1tr1, e1tr2, e1tr3, e2tr1, e2tr2, e2tr3))
        case (Event4Rule(e1tr1, e1tr2, e1tr3, e1tr4), Event2Rule(e2tr1, e2tr2)) =>
          resultRule = Some(Event6Rule(e1tr1, e1tr2, e1tr3, e1tr4, e2tr1, e2tr2))
        case (Event5Rule(e1tr1, e1tr2, e1tr3, e1tr4, e1tr5), Event1Rule(e2tr1)) =>
          resultRule = Some(Event6Rule(e1tr1, e1tr2, e1tr3, e1tr4, e1tr5, e2tr1))
        case _ => sys.error("Rules are not matching the event types")
      }
      resultRule.get.asInstanceOf[Event6Rule]
    }
  }


}

object JoinNode {

  def createWindowEplString(window: Window): String = window match {
    case SlidingInstances(instances) => s"win:length($instances)"
    case TumblingInstances(instances) => s"win:length_batch($instances)"
    case SlidingTime(seconds) => s"win:time($seconds)"
    case TumblingTime(seconds) => s"win:time_batch($seconds)"
  }

}
