package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

class SelectNode(select: Select, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyUnaryNodeStrategy) extends Actor with EsperEngine {

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(select.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val elementIdsEpl: String = select.elementIds.map(i => s"sq.e$i").mkString(", ")

  val eplStatement: EPStatement = createEplStatement(s"select $elementIdsEpl from subquery as sq")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = select.elementIds.map(i => s"sq.e$i").map(newEvents(0).get).toArray
      val subqueryElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(select)
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      context.parent ! event
      if (select.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, select.frequencyRequirement.get)
    }
  })

  val subqueryNode: ActorRef = Node.createChildNodeFrom(select.subquery, nodeName, 1, publishers, context)

  /*val clock: Clock = Clock.systemDefaultZone
  var subqueryLatency: Option[Duration] = None
  var pathLatency: Option[Duration] = None

  def enforceLatencyRequirement(): Unit =  if (select.latencyRequirement.isDefined) select.latencyRequirement.get.operator match {
    case Equal        => if (!(pathLatency.get.compareTo(select.latencyRequirement.get.duration) == 0)) select.latencyRequirement.get.callback(nodeName)
    case NotEqual     => if (!(pathLatency.get.compareTo(select.latencyRequirement.get.duration) != 0)) select.latencyRequirement.get.callback(nodeName)
    case Greater      => if (!(pathLatency.get.compareTo(select.latencyRequirement.get.duration) >  0)) select.latencyRequirement.get.callback(nodeName)
    case GreaterEqual => if (!(pathLatency.get.compareTo(select.latencyRequirement.get.duration) >= 0)) select.latencyRequirement.get.callback(nodeName)
    case Smaller      => if (!(pathLatency.get.compareTo(select.latencyRequirement.get.duration) <  0)) select.latencyRequirement.get.callback(nodeName)
    case SmallerEqual => if (!(pathLatency.get.compareTo(select.latencyRequirement.get.duration) <= 0)) select.latencyRequirement.get.callback(nodeName)
  }*/

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
    /*case LatencyRequest(time) =>
      sender ! LatencyResponse(time)
      subqueryNode ! LatencyRequest(clock.instant)
    case LatencyResponse(requestTime) =>
      subqueryLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (select.subquery.isInstanceOf[Stream]) {
        pathLatency = Some(subqueryLatency.get)
        context.parent ! PathLatency(pathLatency.get)
        enforceLatencyRequirement()
        /* TODOo */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
      }
    case PathLatency(duration) =>
      pathLatency = Some(duration.plus(subqueryLatency.get))
      context.parent ! PathLatency(pathLatency.get)
      enforceLatencyRequirement()
      /* TODOO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")*/
    case Created =>
      context.parent ! Created
      if (select.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, select.frequencyRequirement.get)
      latencyStrategy.onSubtreeCreated(self, subqueryNode, context, nodeName, select.latencyRequirement)
    case unhandledMessage =>
      // TODO
      latencyStrategy.onMessageReceive(unhandledMessage, self, select.subquery, subqueryNode, context, nodeName, select.latencyRequirement)
  }

}
