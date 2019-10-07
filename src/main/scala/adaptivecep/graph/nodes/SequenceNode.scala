package adaptivecep.graph.nodes

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.ConversionRules._
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.encryption.{CryptoAES, Encryption}
import adaptivecep.publishers.Publisher._
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}
import com.espertech.esper.client._
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global

case class SequenceNode(
                         requirements: Set[Requirement],
                         publisherName1: String,
                         publisherName2: String,
                         queryLength1: Int,
                         queryLength2: Int,
                         publishers: Map[String, ActorRef],
                         frequencyMonitorFactory: MonitorFactory,
                         latencyMonitorFactory: MonitorFactory,
                         createdCallback: Option[() => Any],
                         eventCallback: Option[(Event) => Any],
                         privacyContext: Option[PrivacyContext] = None
                         //  ,encryptedEvents: Boolean = false
                       )
  extends LeafNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  val queryPublishers: Array[ActorRef] = Array(publishers(publisherName1), publishers(publisherName2))

  queryPublishers.foreach(_ ! Subscribe)

  var subscription1Acknowledged: Boolean = false
  var subscription2Acknowledged: Boolean = false
  var parentReceived: Boolean = false

  var rule1: Option[EventConversionRule] = None
  var rule2: Option[EventConversionRule] = None
  var resultRule: Option[EventConversionRule] = None
  /***
    * this data is considered trusted as
    */
  val initVector = "ABCDEFGHIJKLMNOP"
  val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
  val secret = "mysecret"
  val spec = new PBEKeySpec(secret.toCharArray, "1234".getBytes(), 65536, 128)
  val factory: SecretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
  val key: Array[Byte] = factory.generateSecret(spec).getEncoded
  val skeySpec = new SecretKeySpec(key, "AES")
  implicit val encryption: Encryption = CryptoAES(skeySpec, iv)

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq.empty)
    case AcknowledgeSubscription(ref) if sender() == queryPublishers(0) =>
      subscription1Acknowledged = true
      println("Acknowledged Subscription 1")
      ref.getSource.to(Sink.foreach(a => {
        processEvent(a, queryPublishers(0))
      })).run(materializer)
    case AcknowledgeSubscription(ref) if sender() == queryPublishers(1) =>
      subscription2Acknowledged = true
      println("Acknowledged Subscription 2")
      ref.getSource.to(Sink.foreach(a => {
        processEvent(a, queryPublishers(1))
      })).run(materializer)

    case CentralizedCreated =>
      if (!created) {
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = LeafNodeData(name, requirements, context, parentNode)
    }
    case SourceRequest =>
      source = Source.queue[Event](20000, OverflowStrategy.dropNew).preMaterialize()(materializer)
      future = source._2.runWith(StreamRefs.sourceRef())(materializer)
      sourceRef = Await.result(future, Duration.Inf)
      sender() ! SourceResponse(sourceRef)
    case KillMe => sender() ! PoisonPill
    case Kill =>
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

  def processEvent(event: Event, sender: ActorRef) = {
    if (sender == queryPublishers(0)) {
      context.system.scheduler.scheduleOnce(
        FiniteDuration(costs(parentNode).duration.toMillis, TimeUnit.MILLISECONDS),
        () => {
          if (parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
            frequencyMonitor.onEventEmit(event, nodeData)
            emittedEvents += 1
            event match {
              case Event1(e1) =>
                sendEvent("sq1", Array(toAnyRef(e1)))
              case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
              case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
              case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
              case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
              case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))

              /** *
                * those cases will not be matched as the publisher will not publish EncEvent
                */
              //              case EncEvent1(e1, rule) =>
              //                if (rule1.isEmpty) rule1 = Some(rule)
              //                sendEvent("sq1", Array(toAnyRef(e1)))
              //              case EncEvent2(e1, e2, rule) =>
              //                if (rule1.isEmpty) rule1 = Some(rule)
              //                sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
              //              case EncEvent3(e1, e2, e3, rule) =>
              //                if (rule1.isEmpty) rule1 = Some(rule)
              //                sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
              //              case EncEvent4(e1, e2, e3, e4, rule) =>
              //                if (rule1.isEmpty) rule1 = Some(rule)
              //                sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
              //              case EncEvent5(e1, e2, e3, e4, e5, rule) =>
              //                if (rule1.isEmpty) rule1 = Some(rule)
              //                sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
              //              case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
              //                if (rule1.isEmpty) rule1 = Some(rule)
              //                sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))

            }
          }
        })
    }
    if (sender == queryPublishers(1)) {
      context.system.scheduler.scheduleOnce(
        FiniteDuration(costs(parentNode).duration.toMillis, TimeUnit.MILLISECONDS),
        () => {
          if (parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
            frequencyMonitor.onEventEmit(event, nodeData)
            emittedEvents += 1
            event match {
              case Event1(e1) => sendEvent("sq2", Array(toAnyRef(e1)))
              case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
              case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
              case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
              case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
              case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))

              /** *
                * those cases will not be matched
                */
              //              case EncEvent1(e1, rule) =>
              //                if (rule2.isEmpty) rule2 = Some(rule)
              //                sendEvent("sq2", Array(toAnyRef(e1)))
              //              case EncEvent2(e1, e2, rule) =>
              //                if (rule2.isEmpty) rule2 = Some(rule)
              //                sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
              //              case EncEvent3(e1, e2, e3, rule) =>
              //                if (rule2.isEmpty) rule2 = Some(rule)
              //                sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
              //              case EncEvent4(e1, e2, e3, e4, rule) =>
              //                if (rule2.isEmpty) rule2 = Some(rule)
              //                sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
              //              case EncEvent5(e1, e2, e3, e4, e5, rule) =>
              //                if (rule2.isEmpty) rule2 = Some(rule)
              //                sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
              //              case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
              //                if (rule2.isEmpty) rule2 = Some(rule)
              //                sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
            }
          }
        })
    }
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

    var encryptedEvents = false
    if (privacyContext.nonEmpty) {
      privacyContext.get match {
        case SgxPrivacyContext(trustedHosts, remoteObject, conversionRules) =>
          if (rule1.isEmpty) rule1 = Some(conversionRules(publisherName1))
          if (rule2.isEmpty) rule2 = Some(conversionRules(publisherName2))
          encryptedEvents = true
        case _ =>
          encryptedEvents = false
      }
    }

//
//    val event: Event = (encryptedEvents, values.length) match {
//      case (false, 2) => Event2(values(0), values(1))
//      case (false, 3) => Event3(values(0), values(1), values(2))
//      case (false, 4) => Event4(values(0), values(1), values(2), values(3))
//      case (false, 5) => Event5(values(0), values(1), values(2), values(3), values(4))
//      case (false, 6) => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
//      case (true, 2) =>
//        getEncryptedEvent(Event2(values(0), values(1)), getResultRule2)
//      //        EncEvent2(values(0), values(1), getResultRule2)
//      case (true, 3) => EncEvent3(values(0), values(1), values(2), getResultRule3)
//      case (true, 4) => EncEvent4(values(0), values(1), values(2), values(3), getResultRule4)
//      case (true, 5) => EncEvent5(values(0), values(1), values(2), values(3), values(4), getResultRule5)
//      case (true, 6) => EncEvent6(values(0), values(1), values(2), values(3), values(4), values(5), getResultRule6)
//    }

    val event: Event = values.length match {
      case 2 =>
        if(!encryptedEvents) Event2(values(0), values(1))
        else getEncryptedEvent(Event2(values(0), values(1)), getResultRule2)
      case 3 =>
        if(!encryptedEvents) Event3(values(0), values(1), values(2))
        else getEncryptedEvent(Event3(values(0), values(1), values(2)), getResultRule3)
      case 4 =>
        if(!encryptedEvents) Event4(values(0), values(1), values(2), values(3))
        else getEncryptedEvent(Event4(values(0), values(1), values(2), values(3)), getResultRule4)
      case 5 =>
        if(!encryptedEvents) Event5(values(0), values(1), values(2), values(3), values(4))
        else getEncryptedEvent(Event5(values(0), values(1), values(2), values(3), values(4)), getResultRule5)
      case 6 =>
        if(!encryptedEvents) Event6(values(0), values(1), values(2), values(3), values(4), values(5))
        else getEncryptedEvent(Event6(values(0), values(1), values(2), values(3), values(4), values(5)), getResultRule6)

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
