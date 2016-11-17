package com.scalarookie.eventscala.actors

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.caseclasses.Query._
import com.scalarookie.eventscala.caseclasses.Event._
import com.scalarookie.eventscala.actors.PublisherActor._

class JoinActor(join: Join, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  val lhsQuery: Query = join.lhsQuery
  val rhsQuery: Query = join.rhsQuery

  val configuration = new Configuration
  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  val lhsFieldNames: Array[String] = (1 until (getNrOfFieldsFrom(lhsQuery) + 1)).map(i => s"p$i").toArray
  val rhsFieldNames: Array[String] = (1 until (getNrOfFieldsFrom(rhsQuery) + 1)).map(i => s"p$i").toArray
  val lhsFieldClasses: Array[java.lang.Class[_]] = getArrayOfClassesFrom(lhsQuery)
  val rhsFieldClasses: Array[java.lang.Class[_]] = getArrayOfClassesFrom(rhsQuery)

  configuration.addEventType("lhs", lhsFieldNames, lhsFieldClasses.asInstanceOf[Array[AnyRef]])
  configuration.addEventType("rhs", rhsFieldNames, rhsFieldClasses.asInstanceOf[Array[AnyRef]])

  def getWindowEplFrom(window: Window): String = window match {
    case Length(length) => s"win:length($length)"
    case LengthBatch(length) => s"win:length_batch($length)"
    case Time(secs) => s"win:time($secs)"
    case TimeBatch(secs) => s"win:time_batch($secs)"
  }

  val lhsWindowEpl = getWindowEplFrom(join.lhsWindow)
  val rhsWindowEpl = getWindowEplFrom(join.rhsWindow)

  val eplStatement = administrator.createEPL(
    s"select * from lhs.$lhsWindowEpl as lhs, rhs.$rhsWindowEpl as rhs")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val lhsValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("lhs").asInstanceOf[Array[AnyRef]]
        val rhsValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("rhs").asInstanceOf[Array[AnyRef]]
        val lhsAndRhsValues: Array[AnyRef] = lhsValues ++ rhsValues
        val lhsAndRhsClasses: Array[java.lang.Class[_]] = lhsFieldClasses ++ rhsFieldClasses
        val event: Event = getEventFrom(lhsAndRhsValues, lhsAndRhsClasses)
        context.parent ! event
        if (root.isEmpty) println(s"Got a result: $event!")
      }
    }
  })

  val x: Option[ActorRef] = Some(self)

  val lhsChild: Option[ActorRef] = lhsQuery match {
    // `lhsQuery` is a subscription to a stream, so we subscribe to it.
    case stream: Stream => publishers(stream.name) ! Subscribe; None
    // `lhsQuery` is another join, so we create a child actor to perform that join.
    case join: Join => Some(context.actorOf(Props(new JoinActor(join, publishers, Some(root.getOrElse(self)))), s"${self.path.name}-lhs"))
  }

  val rhsChild: Option[ActorRef] = rhsQuery match {
    case stream: Stream => publishers(stream.name) ! Subscribe; None
    case join: Join => Some(context.actorOf(Props(new JoinActor(join, publishers, Some(root.getOrElse(self)))), s"${self.path.name}-rhs"))
  }

  // `receive` receives events and figures out whether to send them to the Esper engine as instance of `lhs` or `rhs`.
  override def receive: Receive = {
    case event: Event =>
      val senderName = sender.path.name
      val eventAsArray: Array[AnyRef] = getArrayOfValuesFrom(event)
      (lhsQuery, rhsQuery) match {
        // `lhsQuery` and `rhsQuery` are both subscriptions to the same stream, and `event` is from this stream.
        case (lhsStream: Stream, rhsStream: Stream) if lhsStream.name == rhsStream.name && lhsStream.name == senderName =>
          runtime.sendEvent(eventAsArray, "lhs")
          runtime.sendEvent(eventAsArray, "rhs")
        // `lhsQuery` is a subscription to a stream, and `event` is from this stream.
        case (lhsStream: Stream, _) if lhsStream.name == senderName =>
          runtime.sendEvent(eventAsArray, "lhs")
        // `lhsQuery` is another join, and `event` is resulting from it.
        case (_: Join, _) if senderName == s"${self.path.name}-lhs" =>
          runtime.sendEvent(eventAsArray, "lhs")
        // `rhsQuery` is a subscription to a stream, and `event` is from this stream.
        case (_, rhsStream: Stream) if rhsStream.name == senderName =>
          runtime.sendEvent(eventAsArray, "rhs")
        // `rhsQuery` is another join, and `event` is resulting from it.
        case (_, _: Join) if senderName == s"${self.path.name}-rhs" =>
          runtime.sendEvent(eventAsArray, "rhs")
      }
  }

}
