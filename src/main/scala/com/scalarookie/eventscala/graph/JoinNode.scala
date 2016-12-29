package com.scalarookie.eventscala.graph

import java.time.{Clock, Duration}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyBinaryNodeStrategy}

object JoinNode {

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(instances) => s"win:length($instances)"
    case LengthTumbling(instances) => s"win:length_batch($instances)"
    case TimeSliding(seconds) => s"win:time($seconds)"
    case TimeTumbling(seconds) => s"win:time_batch($seconds)"
  }

}

class JoinNode(join: Join, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyBinaryNodeStrategy) extends Actor with EsperEngine {

  require(join.subquery1 != join.subquery2)

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subquery1ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery1)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery2)
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses)
  addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses)

  val window1Epl: String = JoinNode.getEplFrom(join.window1)
  val window2Epl: String = JoinNode.getEplFrom(join.window2)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery1.$window1Epl as sq1, subquery2.$window2Epl as sq2")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val subquery1ElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("sq1").asInstanceOf[Array[AnyRef]]
        val subquery2ElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("sq2").asInstanceOf[Array[AnyRef]]
        val subqueries1And2ElementValues: Array[AnyRef] = subquery1ElementValues ++ subquery2ElementValues
        val subqueries1And2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
        val event: Event = Event.getEventFrom(subqueries1And2ElementValues, subqueries1And2ElementClasses)
        context.parent ! event
        if (join.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, join.frequencyRequirement.get)
      }
    }
  })

  val subquery1Node: ActorRef = Node.createChildNodeFrom(join.subquery1, nodeName, 1, publishers, context)
  val subquery2Node: ActorRef = Node.createChildNodeFrom(join.subquery2, nodeName, 2, publishers, context)

  /*val clock: Clock = Clock.systemDefaultZone
  var subquery1Latency: Option[Duration] = None
  var subquery2Latency: Option[Duration] = None
  var pathLatency: Option[Duration] = None

  def enforceLatencyRequirement(): Unit =  if (join.latencyRequirement.isDefined) join.latencyRequirement.get.operator match {
    case Equal        => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) == 0)) join.latencyRequirement.get.callback(nodeName)
    case NotEqual     => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) != 0)) join.latencyRequirement.get.callback(nodeName)
    case Greater      => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) >  0)) join.latencyRequirement.get.callback(nodeName)
    case GreaterEqual => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) >= 0)) join.latencyRequirement.get.callback(nodeName)
    case Smaller      => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) <  0)) join.latencyRequirement.get.callback(nodeName)
    case SmallerEqual => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) <= 0)) join.latencyRequirement.get.callback(nodeName)
  }*/

  var oneChildCreated = false

  override def receive: Receive = {
    case event: Event if sender == subquery1Node =>
      sendEvent("subquery1", Event.getArrayOfValuesFrom(event))
    case event: Event if sender == subquery2Node =>
      sendEvent("subquery2", Event.getArrayOfValuesFrom(event))
    /*case LatencyRequest(time) =>
      sender ! LatencyResponse(time)
      subquery1Node ! LatencyRequest(clock.instant)
      subquery2Node ! LatencyRequest(clock.instant)
    case LatencyResponse(requestTime) if sender == subquery1Node =>
      subquery1Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (join.subquery1.isInstanceOf[Stream]) {
        if (pathLatency.isEmpty || subquery1Latency.get.compareTo(pathLatency.get) > 0) {
          pathLatency = subquery1Latency
        }
        context.parent ! PathLatency(pathLatency.get)
        enforceLatencyRequirement()
        /* TODOo */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
      }
    case LatencyResponse(requestTime) if sender == subquery2Node =>
      subquery2Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (join.subquery2.isInstanceOf[Stream]) {
        if (pathLatency.isEmpty || subquery2Latency.get.compareTo(pathLatency.get) > 0) {
          pathLatency = subquery2Latency
        }
        context.parent ! PathLatency(pathLatency.get)
        enforceLatencyRequirement()
        /* TODOo */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
      }
    case PathLatency(duration) if sender == subquery1Node =>
      if (pathLatency.isEmpty || duration.plus(subquery1Latency.get).compareTo(pathLatency.get) > 0) {
        pathLatency = Some(duration.plus(subquery1Latency.get))
      }
      context.parent ! PathLatency(pathLatency.get)
      enforceLatencyRequirement()
      /* TODOo */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
    case PathLatency(duration) if sender == subquery2Node =>
      if (pathLatency.isEmpty || duration.plus(subquery2Latency.get).compareTo(pathLatency.get) > 0) {
        pathLatency = Some(duration.plus(subquery2Latency.get))
      }
      context.parent ! PathLatency(pathLatency.get)
      enforceLatencyRequirement()
      /* TODOo */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")*/
    case Created =>
      if (oneChildCreated) {
        context.parent ! Created
        if (join.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, join.frequencyRequirement.get)
        latencyStrategy.onSubtreeCreated(self, subquery1Node, subquery2Node, context, nodeName, join.latencyRequirement)
      } else {
        oneChildCreated = true
      }
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, self, subquery1Node, subquery2Node, context, nodeName, join.latencyRequirement)
  }

}
