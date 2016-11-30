package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._

object JoinActor {

  def getEplFrom(window: Window): String = window match {
    case LengthSliding(instances) => s"win:length($instances)"
    case LengthTumbling(instances) => s"win:length_batch($instances)"
    case TimeSliding(seconds) => s"win:time($seconds)"
    case TimeTumbling(seconds) => s"win:time_batch($seconds)"
  }

}

class JoinActor(join: Join, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor with EsperEngine {

  require(join.subquery1 != join.subquery2)

  val actorName: String = self.path.name
  override val esperServiceProviderUri: String = actorName

  val subquery1ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery1)
  val subquery1ElementNames: Array[String] = (1 to subquery1ElementClasses.length).map(i => s"e$i").toArray
  val subquery2ElementClasses: Array[Class[_]] = Query.getArrayOfClassesFrom(join.subquery2)
  val subquery2ElementNames: Array[String] = (1 to subquery2ElementClasses.length).map(i => s"e$i").toArray

  addEventType("subquery1", subquery1ElementNames, subquery1ElementClasses)
  addEventType("subquery2", subquery2ElementNames, subquery2ElementClasses)

  val window1Epl: String = JoinActor.getEplFrom(join.window1)
  val window2Epl: String = JoinActor.getEplFrom(join.window2)

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
        if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
      }
    }
  })

  val subquery1Actor: ActorRef = join.subquery1 match {
    case stream1: Stream => context.actorOf(Props(
      new StreamActor(stream1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-stream1")
    case join1: Join if join1.subquery1 == join1.subquery2 => context.actorOf(Props(
      new SelfJoinActor(join1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join1")
    case join1: Join if join1.subquery1 != join1.subquery2 => context.actorOf(Props(
      new JoinActor(join1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join1")
    case select1: Select => context.actorOf(Props(
      new SelectActor(select1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-select1")
    case filter1: Filter => context.actorOf(Props(
      new FilterActor(filter1, publishers, Some(root.getOrElse(self)))),
      s"$actorName-filter1")
  }

  val subquery2Actor: ActorRef = join.subquery2 match {
    case stream2: Stream => context.actorOf(Props(
        new StreamActor(stream2, publishers, Some(root.getOrElse(self)))),
        s"$actorName-stream2")
    case join2: Join if join2.subquery1 == join2.subquery2 => context.actorOf(Props(
      new SelfJoinActor(join2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join2")
    case join2: Join if join2.subquery1 != join2.subquery2 => context.actorOf(Props(
      new JoinActor(join2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-join2")
    case select2: Select => context.actorOf(Props(
      new SelectActor(select2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-select2")
    case filter2: Filter => context.actorOf(Props(
      new FilterActor(filter2, publishers, Some(root.getOrElse(self)))),
      s"$actorName-filter2")
  }

  override def receive: Receive = {
    case event: Event if sender == subquery1Actor =>
      sendEvent("subquery1", Event.getArrayOfValuesFrom(event))
    case event: Event if sender == subquery2Actor =>
      sendEvent("subquery2", Event.getArrayOfValuesFrom(event))
  }

}