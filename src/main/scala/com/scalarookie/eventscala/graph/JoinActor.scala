package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class JoinActor(join: Join, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor with EsperEngine {

  val actorName: String = self.path.name
  override val esperServiceProviderUri: String = actorName

  val subquery1: Query = join.subquery1
  val subquery2: Query = join.subquery2

  val subquery1ElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(subquery1)
  val subquery2ElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(subquery2)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses)
  addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses)

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(instances) => s"win:length($instances)"
    case LengthTumbling(instances) => s"win:length_batch($instances)"
    case TimeSliding(seconds) => s"win:time($seconds)"
    case TimeTumbling(seconds) => s"win:time_batch($seconds)"
  }

  val subquery1WindowEpl: String = getEplFrom(join.subquery1Window)
  val subquery2WindowEpl: String = getEplFrom(join.subquery2Window)

  val eplStatement: EPStatement = createEplStatement(
    s"select * from subquery1.$subquery1WindowEpl as sq1, subquery2.$subquery2WindowEpl as sq2")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val subquery1ElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("sq1").asInstanceOf[Array[AnyRef]]
        val subquery2ElementValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("sq2").asInstanceOf[Array[AnyRef]]
        val subqueries1And2ElementValues: Array[AnyRef] = subquery1ElementValues ++ subquery2ElementValues
        val subqueries1And2ElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(join)
        val event: Event = Event.getEventFrom(subqueries1And2ElementValues, subqueries1And2ElementClasses)
        if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
      }
    }
  })

  val subquery1Actor: ActorRef = subquery1 match {
    case stream1: Stream => context.actorOf(Props(
      new StreamActor(stream1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-stream1")
    case join1: Join => context.actorOf(Props(
      new JoinActor(join1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join1")
    case select1: Select => context.actorOf(Props(
      new SelectActor(select1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-select1")
    case filter1: Filter => context.actorOf(Props(
      new FilterActor(filter1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-filter1")
  }

  val subquery2Actor: Option[ActorRef] = subquery2 match {
    case stream2: Stream => subquery1 match {
      case stream1: Stream if stream1.name == stream2.name => None
      case _ => Some(context.actorOf(Props(
        new StreamActor(stream2, publishers, Some(root.getOrElse(self)))),
        s"$actorName-stream2"))
    }
    case join2: Join => Some(context.actorOf(Props(
      new JoinActor(join2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join2"))
    case select2: Select => Some(context.actorOf(Props(
      new SelectActor(select2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-select2"))
    case filter2: Filter => Some(context.actorOf(Props(
      new FilterActor(filter2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-filter2"))
  }

  override def receive: Receive = {
    case event: Event =>
      if (sender == subquery1Actor) {
        (subquery1, subquery2) match {
          case (stream1: Stream, stream2: Stream) if stream1.name == stream2.name =>
            sendEvent("subquery1", Event.getArrayOfValuesFrom(event))
            sendEvent("subquery2", Event.getArrayOfValuesFrom(event))
          case _ =>
            sendEvent("subquery1", Event.getArrayOfValuesFrom(event))
        }
      } else if (sender == subquery2Actor.get) {
          sendEvent("subquery2", Event.getArrayOfValuesFrom(event))
      }
  }

}
