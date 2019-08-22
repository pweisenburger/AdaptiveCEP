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
  def handleEvent(array: Array[Either[Any, Any]]): Unit = query1 match {
    case 1 =>
      val filledArray: Array[Either[Any, Any]] = fillArray(1, array)
      emitEvent(Event1(filledArray(0)))
    case 2 =>
      val filledArray: Array[Either[Any, Any]] = fillArray(2, array)
      emitEvent(Event2(filledArray(0), filledArray(1)))
    case 3 =>
      val filledArray: Array[Either[Any, Any]] = fillArray(3, array)
      emitEvent(Event3(filledArray(0), filledArray(1), filledArray(2)))
    case 4 =>
      val filledArray: Array[Either[Any, Any]] = fillArray(4, array)
      emitEvent(Event4(filledArray(0), filledArray(1), filledArray(2), filledArray(3)))
    case 5 =>
      val filledArray: Array[Either[Any, Any]] = fillArray(5, array)
      emitEvent(Event5(filledArray(0), filledArray(1), filledArray(2), filledArray(3), filledArray(4)))
    case 6 =>
      val filledArray: Array[Either[Any, Any]] = fillArray(6, array)
      emitEvent(Event6(filledArray(0), filledArray(1), filledArray(2), filledArray(3), filledArray(4), filledArray(5)))
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
      if(!created){
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
      ref.getSource.to(Sink foreach(e =>{
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
      if(childNode1.eq(old)){childNode1 = a}
      if(childNode2.eq(old)){childNode2 = a}
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      scheduledTask.cancel()
      if(lmonitor.isDefined) lmonitor.get.scheduledTask.cancel()
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
    if(sender == childNode1) {
      event match {
        case Event1(e1) => handleEvent(Array(Left(e1)))
        case Event2(e1, e2) => handleEvent(Array(Left(e1), Left(e2)))
        case Event3(e1, e2, e3) => handleEvent(Array(Left(e1), Left(e2), Left(e3)))
        case Event4(e1, e2, e3, e4) => handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4)))
        case Event5(e1, e2, e3, e4, e5) => handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4), Left(e5)))
        case Event6(e1, e2, e3, e4, e5, e6) => handleEvent(Array(Left(e1), Left(e2), Left(e3), Left(e4), Left(e5), Left(e6)))
        case EncEvent1(e1, rule) => handleEvent(Array(Right(e1)))
          if (leftRule.isEmpty) leftRule = Some(rule)
        case EncEvent2(e1, e2, rule) => handleEvent(Array(Right(e1), Right(e2)))
          if (leftRule.isEmpty) leftRule = Some(rule)
        case EncEvent3(e1, e2, e3, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3)))
          if (leftRule.isEmpty) leftRule = Some(rule)
        case EncEvent4(e1, e2, e3, e4, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4)))
          if (leftRule.isEmpty) leftRule = Some(rule)
        case EncEvent5(e1, e2, e3, e4, e5, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5)))
          if (leftRule.isEmpty) leftRule = Some(rule)
        case EncEvent6(e1, e2, e3, e4, e5, e6, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5), Right(e6)))
          if (leftRule.isEmpty) leftRule = Some(rule)
      }
    }
    else if(sender == childNode2){
     event match {
      case Event1(e1) => handleEvent(Array(Right(e1)))
      case Event2(e1, e2) => handleEvent(Array(Right(e1), Right(e2)))
      case Event3(e1, e2, e3) => handleEvent(Array(Right(e1), Right(e2), Right(e3)))
      case Event4(e1, e2, e3, e4) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4)))
      case Event5(e1, e2, e3, e4, e5) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5), Right(e6)))
      case EncEvent1(e1, rule) => handleEvent(Array(Right(e1)))
        if (rightRule.isEmpty) rightRule = Some(rule)
      case EncEvent2(e1, e2, rule) => handleEvent(Array(Right(e1), Right(e2)))
        if (rightRule.isEmpty) rightRule = Some(rule)
      case EncEvent3(e1, e2, e3, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3)))
        if (rightRule.isEmpty) rightRule = Some(rule)
      case EncEvent4(e1, e2, e3, e4, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4)))
        if (rightRule.isEmpty) rightRule = Some(rule)
      case EncEvent5(e1, e2, e3, e4, e5, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5)))
        if (rightRule.isEmpty) rightRule = Some(rule)
      case EncEvent6(e1, e2, e3, e4, e5, e6, rule) => handleEvent(Array(Right(e1), Right(e2), Right(e3), Right(e4), Right(e5), Right(e6)))
        if (rightRule.isEmpty) rightRule = Some(rule)
     }
  }
  }
}
