package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

class SelfJoinNode(join: Join, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyUnaryNodeStrategy) extends Actor with EsperEngine {

  require(join.subquery1 == join.subquery2)

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery1)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val window1Epl: String = JoinNode.getEplFrom(join.window1)
  val window2Epl: String = JoinNode.getEplFrom(join.window2)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery.$window1Epl as lhs, subquery.$window2Epl as rhs")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val lhsElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("lhs").asInstanceOf[Array[AnyRef]]
        val rhsElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("rhs").asInstanceOf[Array[AnyRef]]
        val lhsAndRhsElementValues: Array[AnyRef] = lhsElementValues ++ rhsElementValues
        val lhsAndRhsElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join)
        val event: Event = Event.getEventFrom(lhsAndRhsElementValues, lhsAndRhsElementClasses)
        context.parent ! event
        if (join.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, join.frequencyRequirement.get)
      }
    }
  })

  val subqueryNode: ActorRef = Node.createChildNodeFrom(join.subquery1, nodeName, 1, publishers, context)

  /*val clock: Clock = Clock.systemDefaultZone
  var subqueryLatency: Option[Duration] = None
  var pathLatency: Option[Duration] = None

  def enforceLatencyRequirement(): Unit =  if (join.latencyRequirement.isDefined) join.latencyRequirement.get.operator match {
    case Equal        => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) == 0)) join.latencyRequirement.get.callback(nodeName)
    case NotEqual     => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) != 0)) join.latencyRequirement.get.callback(nodeName)
    case Greater      => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) >  0)) join.latencyRequirement.get.callback(nodeName)
    case GreaterEqual => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) >= 0)) join.latencyRequirement.get.callback(nodeName)
    case Smaller      => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) <  0)) join.latencyRequirement.get.callback(nodeName)
    case SmallerEqual => if (!(pathLatency.get.compareTo(join.latencyRequirement.get.duration) <= 0)) join.latencyRequirement.get.callback(nodeName)
  }*/

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
    /*case LatencyRequest(time) =>
      sender ! LatencyResponse(time)
      subqueryNode ! LatencyRequest(clock.instant)
    case LatencyResponse(requestTime) =>
      subqueryLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (join.subquery1.isInstanceOf[Stream]) {
        pathLatency = Some(subqueryLatency.get)
        context.parent ! PathLatency(pathLatency.get)
        enforceLatencyRequirement()
        /* TODOO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
      }
    case PathLatency(duration) =>
      pathLatency = Some(duration.plus(subqueryLatency.get))
      context.parent ! PathLatency(pathLatency.get)
      enforceLatencyRequirement()
      /* TODOO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")*/
    case Created =>
      context.parent ! Created
      if (join.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, join.frequencyRequirement.get)
      latencyStrategy.onSubtreeCreated(self, subqueryNode, context, nodeName, join.latencyRequirement)
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, self, join.subquery1, subqueryNode, context, nodeName, join.latencyRequirement)

  }

}
