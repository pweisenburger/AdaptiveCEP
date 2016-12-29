package com.scalarookie.eventscala.graph

import java.time.{Clock, Duration}

import akka.actor.{Actor, ActorRef}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.qos.{FrequencyStrategy, PathLatencyUnaryNodeStrategy}

object FilterNode {

  def getEplFrom(operand: Either[Int, Any]): String = operand match {
    case Left(id) => s"sq.e$id"
    case Right(literal) => literal match {
      // Have a look at "Table 5.1. Types of EPL constants". `java.lang.Byte` has been omitted for simplicity's sake.
      // http://www.espertech.com/esper/release-5.5.0/esper-reference/html/epl_clauses.html#epl-syntax-datatype
      case s: String => s"'$s'" // TODO ???
      case i: Integer => i.toString
      case l: java.lang.Long => s"${l}l"
      case f: java.lang.Float => s"${f}f"
      case d: java.lang.Double => d.toString
      case b: java.lang.Boolean => b.toString
    }
  }

  def getEplFrom(operator: Operator): String = operator match {
    case Equal => "="
    case NotEqual => "!="
    case Greater => ">"
    case GreaterEqual => ">="
    case Smaller => "<"
    case SmallerEqual => "<="
  }

}

class FilterNode(filter: Filter, publishers: Map[String, ActorRef], frequencyStrategy: FrequencyStrategy, latencyStrategy: PathLatencyUnaryNodeStrategy) extends Actor with EsperEngine {

  val nodeName: String = self.path.name
  override val esperServiceProviderUri: String = nodeName

  val subqueryElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(filter.subquery)
  val subqueryElementNames: Array[String] = (1 to subqueryElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery", subqueryElementNames, subqueryElementClasses)

  val operand1Epl: String = FilterNode.getEplFrom(filter.operand1)
  val operand2Epl: String = FilterNode.getEplFrom(filter.operand2)

  val operatorEpl: String = FilterNode.getEplFrom(filter.operator)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery as sq where $operand1Epl $operatorEpl $operand2Epl")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      val subqueryElementValues: Array[AnyRef] = subqueryElementNames.map(newEvents(0).get)
      val event: Event = Event.getEventFrom(subqueryElementValues, subqueryElementClasses)
      context.parent ! event
      if (filter.frequencyRequirement.isDefined) frequencyStrategy.onEventEmit(context, nodeName, filter.frequencyRequirement.get)
    }
  })

  val subqueryNode: ActorRef = Node.createChildNodeFrom(filter.subquery, nodeName, 1, publishers, context)

  /*val clock: Clock = Clock.systemDefaultZone
  var subqueryLatency: Option[Duration] = None
  var pathLatency: Option[Duration] = None

  def enforceLatencyRequirement(): Unit =  if (filter.latencyRequirement.isDefined) filter.latencyRequirement.get.operator match {
    case Equal        => if (!(pathLatency.get.compareTo(filter.latencyRequirement.get.duration) == 0)) filter.latencyRequirement.get.callback(nodeName)
    case NotEqual     => if (!(pathLatency.get.compareTo(filter.latencyRequirement.get.duration) != 0)) filter.latencyRequirement.get.callback(nodeName)
    case Greater      => if (!(pathLatency.get.compareTo(filter.latencyRequirement.get.duration) >  0)) filter.latencyRequirement.get.callback(nodeName)
    case GreaterEqual => if (!(pathLatency.get.compareTo(filter.latencyRequirement.get.duration) >= 0)) filter.latencyRequirement.get.callback(nodeName)
    case Smaller      => if (!(pathLatency.get.compareTo(filter.latencyRequirement.get.duration) <  0)) filter.latencyRequirement.get.callback(nodeName)
    case SmallerEqual => if (!(pathLatency.get.compareTo(filter.latencyRequirement.get.duration) <= 0)) filter.latencyRequirement.get.callback(nodeName)
  }*/

  override def receive: Receive = {
    case event: Event if sender == subqueryNode =>
      sendEvent("subquery", Event.getArrayOfValuesFrom(event))
    /*case LatencyRequest(time) =>
      sender ! LatencyResponse(time)
      subqueryNode ! LatencyRequest(clock.instant)
    case LatencyResponse(requestTime) =>
      subqueryLatency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
      if (filter.subquery.isInstanceOf[Stream]) {
        pathLatency = Some(subqueryLatency.get)
        context.parent ! PathLatency(pathLatency.get)
        enforceLatencyRequirement()
        /* TODOO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}")
      }
    case PathLatency(duration) =>
      pathLatency = Some(duration.plus(subqueryLatency.get))
      enforceLatencyRequirement()
      context.parent ! PathLatency(pathLatency.get)
      enforceLatencyRequirement()
      /* TODOO */ println(s"PATH LATENCY:\t\tNode $nodeName: ${pathLatency.get}") */
    case Created =>
      context.parent ! Created
      if (filter.frequencyRequirement.isDefined) frequencyStrategy.onSubtreeCreated(context, nodeName, filter.frequencyRequirement.get)
      latencyStrategy.onSubtreeCreated(self, subqueryNode, context, nodeName, filter.latencyRequirement)
    case unhandledMessage =>
      latencyStrategy.onMessageReceive(unhandledMessage, self, filter.subquery, subqueryNode, context, nodeName, filter.latencyRequirement)
  }

}
