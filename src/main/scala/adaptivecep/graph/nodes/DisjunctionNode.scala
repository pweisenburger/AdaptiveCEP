package adaptivecep.graph.nodes

import akka.actor.{ActorRef, Address, Deploy, PoisonPill, Props}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import adaptivecep.privacy.ConversionRules._
import akka.remote.RemoteScope
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class DisjunctionNode(
                            requirements: Set[Requirement],
                            query1: Int,
                            publishers: Map[String, ActorRef],
                            frequencyMonitorFactory: MonitorFactory,
                            latencyMonitorFactory: MonitorFactory,
                            createdCallback: Option[() => Any],
                            eventCallback: Option[(Event) => Any],
                            encryptedEvents: Boolean = false)
  extends BinaryNode {

  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false
  var parentReceived: Boolean = false

  var leftRule: Option[EventConversionRule] = None
  var rightRule: Option[EventConversionRule] = None
  var resultRule: Option[EventConversionRule] = None

  def fillArray(desiredLength: Int, array: Array[Either[Any, Any]]): Array[Either[Any, Any]] = {
    require(array.length <= desiredLength)
    require(array.length > 0)
    val unit: Either[Unit, Unit] = array(0) match {
      case Left(_) => Left(())
      case Right(_) => Right(())
    }
    (0 until desiredLength).map(i => {
      if (i < array.length) {
        array(i)
      } else {
        unit
      }
    }).toArray
  }

  def handleEvent(array: Array[Either[Any, Any]]): Unit =
    (query1, encryptedEvents) match {
      case (1, false) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(1, array)
        emitEvent(Event1(filledArray(0)))
      case (2, false) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(2, array)
        emitEvent(Event2(filledArray(0), filledArray(1)))
      case (3, false) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(3, array)
        emitEvent(Event3(filledArray(0), filledArray(1), filledArray(2)))
      case (4, false) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(4, array)
        emitEvent(Event4(filledArray(0), filledArray(1), filledArray(2), filledArray(3)))
      case (5, false) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(5, array)
        emitEvent(Event5(filledArray(0), filledArray(1), filledArray(2), filledArray(3), filledArray(4)))
      case (6, false) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(6, array)
        emitEvent(Event6(filledArray(0), filledArray(1), filledArray(2), filledArray(3), filledArray(4), filledArray(5)))
      case (1, true) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(1, array)
        emitEvent(EncEvent1(filledArray(0), getEncRule1))
      case (2, true) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(2, array)
        emitEvent(EncEvent2(filledArray(0), filledArray(1), getEncRule2))
      case (3, true) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(3, array)
        emitEvent(EncEvent3(filledArray(0), filledArray(1), filledArray(2), getEncRule3))
      case (4, true) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(4, array)
        emitEvent(EncEvent4(filledArray(0), filledArray(1), filledArray(2), filledArray(3), getEncRule4))
      case (5, true) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(5, array)
        emitEvent(EncEvent5(filledArray(0), filledArray(1), filledArray(2), filledArray(3), filledArray(4), getEncRule5))
      case (6, true) =>
        val filledArray: Array[Either[Any, Any]] = fillArray(6, array)
        emitEvent(EncEvent6(filledArray(0), filledArray(1), filledArray(2), filledArray(3), filledArray(4), filledArray(5), getEncRule6))
    }


  private def getEncRule1: Event1Rule = {
    if (resultRule.isEmpty)
      (leftRule.get, rightRule.get) match {
        case (Event1Rule(tr1), Event1Rule(tr2)) =>
          resultRule = Some(Event1Rule(DisjunctionTransformer(tr1, tr2)))
        case _ => sys.error("error at getEncRule1 Disjuction node, non of the cases where matched!")
      }
    resultRule.get.asInstanceOf[Event1Rule]
  }

  private def getEncRule2: Event2Rule = {
    if (resultRule.isEmpty)
      (leftRule.get, rightRule.get) match {
        case (Event2Rule(ltr1, ltr2), Event1Rule(rtr1)) =>
          resultRule = Some(Event2Rule(DisjunctionTransformer(ltr1, rtr1), DisjunctionTransformer(ltr2, NoTransformer)))
        case (Event1Rule(ltr1), Event2Rule(rtr1, rtr2)) =>
          resultRule = Some(Event2Rule(DisjunctionTransformer(ltr1, rtr1), DisjunctionTransformer(NoTransformer, rtr2)))
        case (Event2Rule(ltr1, ltr2), Event2Rule(rtr1, rtr2)) =>
          resultRule = Some(Event2Rule(DisjunctionTransformer(ltr1, rtr1), DisjunctionTransformer(ltr2, rtr2)))
        case _ => sys.error("error at getEncRule2 Disjuction node, non of the cases where matched!")
      }
    resultRule.get.asInstanceOf[Event2Rule]
  }

  private def getEncRule3: Event3Rule = {
    if (resultRule.isEmpty)
      (leftRule.get, rightRule.get) match {
        case (Event1Rule(ltr1), Event3Rule(rtr1, rtr2, rtr3)) =>
          resultRule = Some(Event3Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(NoTransformer, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3)
          ))
        case (Event2Rule(ltr1, ltr2), Event3Rule(rtr1, rtr2, rtr3)) =>
          resultRule = Some(Event3Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3)
          ))
        case (Event3Rule(ltr1, ltr2, ltr3), Event1Rule(rtr1)) =>
          resultRule = Some(Event3Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, NoTransformer),
            DisjunctionTransformer(ltr3, NoTransformer)
          ))
        case (Event3Rule(ltr1, ltr2, ltr3), Event2Rule(rtr1, rtr2)) =>
          resultRule = Some(Event3Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, NoTransformer)
          ))
        case (Event3Rule(ltr1, ltr2, ltr3), Event3Rule(rtr1, rtr2, rtr3)) =>
          resultRule = Some(Event3Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3)
          ))
        case _ => sys.error("error at getEncRule3 Disjuction node, non of the cases where matched!")
      }

    resultRule.get.asInstanceOf[Event3Rule]
  }

  private def getEncRule4: Event4Rule = {
    if (resultRule.isEmpty)
      (leftRule.get, rightRule.get) match {
        case (Event1Rule(ltr1), Event4Rule(rtr1, rtr2, rtr3, rtr4)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(NoTransformer, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4)
          ))
        case (Event2Rule(ltr1, ltr2), Event4Rule(rtr1, rtr2, rtr3, rtr4)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4)
          ))
        case (Event3Rule(ltr1, ltr2, ltr3), Event4Rule(rtr1, rtr2, rtr3, rtr4)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4)
          ))
        case (Event4Rule(ltr1, ltr2, ltr3, ltr4), Event3Rule(rtr1, rtr2, rtr3)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, NoTransformer)
          ))
        case (Event4Rule(ltr1, ltr2, ltr3, ltr4), Event2Rule(rtr1, rtr2)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, NoTransformer),
            DisjunctionTransformer(ltr4, NoTransformer)
          ))
        case (Event4Rule(ltr1, ltr2, ltr3, ltr4), Event1Rule(rtr1)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, NoTransformer),
            DisjunctionTransformer(ltr3, NoTransformer),
            DisjunctionTransformer(ltr4, NoTransformer)
          ))
        case (Event4Rule(ltr1, ltr2, ltr3, ltr4), Event4Rule(rtr1, rtr2, rtr3, rtr4)) =>
          resultRule = Some(Event4Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4)
          ))
        case _ => sys.error("error at getEncRule4 Disjuction node, non of the cases where matched!")
      }
    resultRule.get.asInstanceOf[Event4Rule]
  }

  private def getEncRule5: Event5Rule = {
    if (resultRule.isEmpty)
      (leftRule.get, rightRule.get) match {
        //TODO: cases 1/5 2/5 3/5 4/5 5/4 5/3 5/2 5/1
        case (Event1Rule(ltr1), Event5Rule(rtr1, rtr2, rtr3, rtr4, rtr5)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(NoTransformer, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5)
          ))
        case (Event2Rule(ltr1, ltr2), Event5Rule(rtr1, rtr2, rtr3, rtr4, rtr5)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5)
          ))
        case (Event3Rule(ltr1, ltr2, ltr3), Event5Rule(rtr1, rtr2, rtr3, rtr4, rtr5)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5)
          ))
        case (Event4Rule(ltr1, ltr2, ltr3, ltr4), Event5Rule(rtr1, rtr2, rtr3, rtr4, rtr5)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5)
          ))
        case (Event5Rule(ltr1, ltr2, ltr3, ltr4, ltr5), Event4Rule(rtr1, rtr2, rtr3, rtr4)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(ltr5, NoTransformer)
          ))
        case (Event5Rule(ltr1, ltr2, ltr3, ltr4, ltr5), Event3Rule(rtr1, rtr2, rtr3)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, NoTransformer),
            DisjunctionTransformer(ltr5, NoTransformer)
          ))
        case (Event5Rule(ltr1, ltr2, ltr3, ltr4, ltr5), Event2Rule(rtr1, rtr2)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, NoTransformer),
            DisjunctionTransformer(ltr4, NoTransformer),
            DisjunctionTransformer(ltr5, NoTransformer)
          ))
        case (Event5Rule(ltr1, ltr2, ltr3, ltr4, ltr5), Event1Rule(rtr1)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, NoTransformer),
            DisjunctionTransformer(ltr3, NoTransformer),
            DisjunctionTransformer(ltr4, NoTransformer),
            DisjunctionTransformer(ltr5, NoTransformer)
          ))
        case (Event5Rule(ltr1, ltr2, ltr3, ltr4, ltr5), Event5Rule(rtr1, rtr2, rtr3, rtr4, rtr5)) =>
          resultRule = Some(Event5Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(ltr5, rtr5)
          ))
        case _ => sys.error("error at getEncRule5 Disjuction node, non of the cases where matched!")
      }
    resultRule.get.asInstanceOf[Event5Rule]
  }

  private def getEncRule6: Event6Rule = {
    if (resultRule.isEmpty)
      (leftRule.get, rightRule.get) match {
        //TODO: cases 1/6 2/6 3/6 4/6 5/6 6/5 6/4 6/3 6/2 6/1
        case (Event1Rule(ltr1), Event6Rule(rtr1, rtr2, rtr3, rtr4, rtr5, rtr6)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(NoTransformer, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5),
            DisjunctionTransformer(NoTransformer, rtr6)
          ))
        case (Event2Rule(ltr1, ltr2), Event6Rule(rtr1, rtr2, rtr3, rtr4, rtr5, rtr6)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(NoTransformer, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5),
            DisjunctionTransformer(NoTransformer, rtr6)
          ))
        case (Event3Rule(ltr1, ltr2, ltr3), Event6Rule(rtr1, rtr2, rtr3, rtr4, rtr5, rtr6)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(NoTransformer, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5),
            DisjunctionTransformer(NoTransformer, rtr6)
          ))
        case (Event4Rule(ltr1, ltr2, ltr3, ltr4), Event6Rule(rtr1, rtr2, rtr3, rtr4, rtr5, rtr6)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(NoTransformer, rtr5),
            DisjunctionTransformer(NoTransformer, rtr6)
          ))
        case (Event5Rule(ltr1, ltr2, ltr3, ltr4, ltr5), Event6Rule(rtr1, rtr2, rtr3, rtr4, rtr5, rtr6)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(ltr5, rtr5),
            DisjunctionTransformer(NoTransformer, rtr6)
          ))
        case (Event6Rule(ltr1, ltr2, ltr3, ltr4, ltr5, ltr6), Event5Rule(rtr1, rtr2, rtr3, rtr4, rtr5)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(ltr5, rtr5),
            DisjunctionTransformer(ltr6, NoTransformer)
          ))
        case (Event6Rule(ltr1, ltr2, ltr3, ltr4, ltr5, ltr6), Event4Rule(rtr1, rtr2, rtr3, rtr4)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(ltr5, NoTransformer),
            DisjunctionTransformer(ltr6, NoTransformer)
          ))
        case (Event6Rule(ltr1, ltr2, ltr3, ltr4, ltr5, ltr6), Event3Rule(rtr1, rtr2, rtr3)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, NoTransformer),
            DisjunctionTransformer(ltr5, NoTransformer),
            DisjunctionTransformer(ltr6, NoTransformer)
          ))
        case (Event6Rule(ltr1, ltr2, ltr3, ltr4, ltr5, ltr6), Event2Rule(rtr1, rtr2)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, NoTransformer),
            DisjunctionTransformer(ltr4, NoTransformer),
            DisjunctionTransformer(ltr5, NoTransformer),
            DisjunctionTransformer(ltr6, NoTransformer)
          ))
        case (Event6Rule(ltr1, ltr2, ltr3, ltr4, ltr5, ltr6), Event1Rule(rtr1)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, NoTransformer),
            DisjunctionTransformer(ltr3, NoTransformer),
            DisjunctionTransformer(ltr4, NoTransformer),
            DisjunctionTransformer(ltr5, NoTransformer),
            DisjunctionTransformer(ltr6, NoTransformer)
          ))
        case (Event6Rule(ltr1, ltr2, ltr3, ltr4, ltr5, ltr6), Event6Rule(rtr1, rtr2, rtr3, rtr4, rtr5, rtr6)) =>
          resultRule = Some(Event6Rule(
            DisjunctionTransformer(ltr1, rtr1),
            DisjunctionTransformer(ltr2, rtr2),
            DisjunctionTransformer(ltr3, rtr3),
            DisjunctionTransformer(ltr4, rtr4),
            DisjunctionTransformer(ltr5, rtr5),
            DisjunctionTransformer(ltr6, rtr6)
          ))
        case _ => sys.error("error at getEncRule6 Disjuction node, non of the cases where matched!")
      }
    resultRule.get.asInstanceOf[Event6Rule]
  }


  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode1, childNode2))
    case Created if sender() == childNode1 =>
      childNode1Created = true
    //if (childNode2Created && parentReceived && !created) emitCreated()
    case Created if sender() == childNode2 =>
      childNode2Created = true
    //if (childNode1Created && parentReceived && !created) emitCreated()
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

  def processEvent(event: Event, sender: ActorRef): Unit = {
    processedEvents += 1
    if (sender == childNode1) {
      event match {
        case Event1(e1) => handleEvent(Array(Left(e1)))
        case Event2(e1, e2) => handleEvent(Array(Left(e1), Left(e2)))
        case Event3(e1, e2, e3) => handleEvent(Array(Left(e1), Left(e2), Left(e3)))
        case Event4(e1, e2, e3, e4) => handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4)))
        case Event5(e1, e2, e3, e4, e5) => handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4), Left(e5)))
        case Event6(e1, e2, e3, e4, e5, e6) => handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4), Left(e5), Left(e6)))
        case EncEvent1(e1, rule) =>
          if (leftRule.isEmpty) leftRule = Some(rule)
          handleEvent(Array(Left(e1)))
        case EncEvent2(e1, e2, rule) =>
          if (leftRule.isEmpty) leftRule = Some(rule)
          handleEvent(Array(Left(e1), Left(e2)))
        case EncEvent3(e1, e2, e3, rule) =>
          if (leftRule.isEmpty) leftRule = Some(rule)
          handleEvent(Array(Left(e1), Left(e2), Left(e3)))
        case EncEvent4(e1, e2, e3, e4, rule) =>
          if (leftRule.isEmpty) leftRule = Some(rule)
          handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4)))
        case EncEvent5(e1, e2, e3, e4, e5, rule) =>
          if (leftRule.isEmpty) leftRule = Some(rule)
          handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4), Left(e5)))
        case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
          if (leftRule.isEmpty) leftRule = Some(rule)
          handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4), Left(e5), Left(e6)))
      }
    }
    else if (sender == childNode2) {
      event match {
        case Event1(e1) => handleEvent(Array(Right(e1)))
        case Event2(e1, e2) => handleEvent(Array(Right(e1), Right(e2)))
        case Event3(e1, e2, e3) => handleEvent(Array(Right(e1), Right(e2), Right(e3)))
        case Event4(e1, e2, e3, e4) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4)))
        case Event5(e1, e2, e3, e4, e5) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5)))
        case Event6(e1, e2, e3, e4, e5, e6) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5), Right(e6)))
        case EncEvent1(e1, rule) =>
          if (rightRule.isEmpty) rightRule = Some(rule)
          handleEvent(Array(Right(e1)))
        case EncEvent2(e1, e2, rule) =>
          if (rightRule.isEmpty) rightRule = Some(rule)
          handleEvent(Array(Right(e1), Right(e2)))
        case EncEvent3(e1, e2, e3, rule) =>
          if (rightRule.isEmpty) rightRule = Some(rule)
          handleEvent(Array(Right(e1), Right(e2), Right(e3)))
        case EncEvent4(e1, e2, e3, e4, rule) =>
          if (rightRule.isEmpty) rightRule = Some(rule)
          handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4)))
        case EncEvent5(e1, e2, e3, e4, e5, rule) =>
          if (rightRule.isEmpty) rightRule = Some(rule)
          handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5)))
        case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
          if (rightRule.isEmpty) rightRule = Some(rule)
          handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5), Right(e6)))
      }
    }
  }
}
