package com.scalarookie.eventscala.actors

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

class JoinActor(join: Join, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  /* TODO */ println(s"Node `${self.path.name}` created; representing `$join`.")

  val subquery1: Query = join.subquery1
  val subquery2: Query = join.subquery2

  val configuration = new Configuration
  lazy val serviceProvider = EPServiceProviderManager.getProvider(s"${self.path.name}", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  val subquery1ElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(subquery1)
  val subquery2ElementClasses: Array[java.lang.Class[_]] = Query.getArrayOfClassesFrom(subquery2)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  configuration.addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses.asInstanceOf[Array[AnyRef]])
  configuration.addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses.asInstanceOf[Array[AnyRef]])

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(length) => s"win:length($length)"
    case LengthTumbling(length) => s"win:length_batch($length)"
    case TimeSliding(secs) => s"win:time($secs)"
    case TimeTumbling(secs) => s"win:time_batch($secs)"
  }

  val subquery1WindowEpl: String = getEplFrom(join.subquery1Window)
  val subquery2WindowEpl: String = getEplFrom(join.subquery2Window)

  val eplStatement: EPStatement = administrator.createEPL(
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
      s"${self.path.name}-stream1")
    case join1: Join => context.actorOf(Props(
      new JoinActor(join1, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-join1")
    case select1: Select => context.actorOf(Props(
      new SelectActor(select1, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-select1")
    case filter1: Filter => context.actorOf(Props(
      new FilterActor(filter1, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-filter1")
  }

  val subquery2Actor: Option[ActorRef] = subquery2 match {
    case stream2: Stream => subquery1 match {
      case stream1: Stream if stream1.name == stream2.name => None
      case _ => Some(context.actorOf(Props(
        new StreamActor(stream2, publishers, Some(root.getOrElse(self)))),
        s"${self.path.name}-stream2"))
    }
    case join2: Join => Some(context.actorOf(Props(
      new JoinActor(join2, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-join2"))
    case select2: Select => Some(context.actorOf(Props(
      new SelectActor(select2, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-select2"))
    case filter2: Filter => Some(context.actorOf(Props(
      new FilterActor(filter2, publishers, Some(root.getOrElse(self)))),
      s"${self.path.name}-filter2"))
  }

  override def receive: Receive = {
    case event: Event =>
      if (sender == subquery1Actor) {
        (subquery1, subquery2) match {
          case (stream1: Stream, stream2: Stream) if stream1.name == stream2.name =>
            runtime.sendEvent(Event.getArrayOfValuesFrom(event), "subquery1")
            runtime.sendEvent(Event.getArrayOfValuesFrom(event), "subquery2")
          case _ =>
            runtime.sendEvent(Event.getArrayOfValuesFrom(event), "subquery1")
        }
      } else if (sender == subquery2Actor.get) {
          runtime.sendEvent(Event.getArrayOfValuesFrom(event), "subquery2")
      }
  }

}
