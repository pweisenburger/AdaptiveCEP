package com.scalarookie.eventscala.actors

import akka.actor.{Actor, ActorRef, Props}
import com.espertech.esper.client._
import com.scalarookie.eventscala.caseclasses._
import com.scalarookie.eventscala.caseclasses.Query._
import com.scalarookie.eventscala.caseclasses.Event._
import com.scalarookie.eventscala.actors.PublisherActor._

// `join` is the query this node represents, `publishers` contains the `ActorRef`s of all `Publisher`s of `Event`
// instances, and `root` is the `ActorRef` of the root node of the event graph -- iff this node is not root itself.
class JoinActor(join: Join, publishers: Map[String, ActorRef], root: Option[ActorRef]) extends Actor {

  // For convenience, `lhsQuery` and `rhsQuery` are pulled out of the query.
  val lhsQuery: Query = join.lhsQuery
  val rhsQuery: Query = join.rhsQuery

  // Boilerplate code required by Esper
  val configuration = new Configuration
  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  // Esper doesn't work with generic classes, so we have to tell it about the types like so:
  val lhsClasses: Array[java.lang.Class[_]] = getArrayOfClassesFrom(lhsQuery)
  val rhsClasses: Array[java.lang.Class[_]] = getArrayOfClassesFrom(rhsQuery)
  // EventScala represents events as tuples, not classes. Esper expects field names in the style of POJOs, though.
  // As a fix, we provide dummy names. E.g., for a `Tuple2` we provide `Array("p1", "p2")`
  val lhsNames: Array[String] = (1 to lhsClasses.length).map(i => s"p$i").toArray
  val rhsNames: Array[String] = (1 to rhsClasses.length).map(i => s"p$i").toArray

  configuration.addEventType("lhs", lhsNames, lhsClasses.asInstanceOf[Array[AnyRef]])
  configuration.addEventType("rhs", rhsNames, rhsClasses.asInstanceOf[Array[AnyRef]])

  // `getEplFrom(window: Window)` obviously generates an EPL string from a given `Window` instance.
  def getEplFrom(window: Window): String = window match {
    case Length(length) => s"win:length($length)"
    case LengthBatch(length) => s"win:length_batch($length)"
    case Time(secs) => s"win:time($secs)"
    case TimeBatch(secs) => s"win:time_batch($secs)"
  }

  val lhsWindowEpl = getEplFrom(join.lhsWindow)
  val rhsWindowEpl = getEplFrom(join.rhsWindow)

  // Obviously, below we register an EPL statement representing the join with Esper...
  val eplStatement = administrator.createEPL(
    s"select * from lhs.$lhsWindowEpl as lhs, rhs.$rhsWindowEpl as rhs")

  // ... and add an `UpdateListener` to it, specifying what to do when Esper emits new events.
  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      // A join usually results in several new events, so we reify them one by one.
      for (nrOfNewEvent <- newEvents.indices) {
        val lhsValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("lhs").asInstanceOf[Array[AnyRef]]
        val rhsValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("rhs").asInstanceOf[Array[AnyRef]]
        val lhsAndRhsValues: Array[AnyRef] = lhsValues ++ rhsValues
        val lhsAndRhsClasses: Array[java.lang.Class[_]] = lhsClasses ++ rhsClasses
        // From an array of `AnyRef`s and an array of the corresponding `Class`es, we reify an `Event` instance.
        val event: Event = getEventFrom(lhsAndRhsValues, lhsAndRhsClasses)
        // If this node is the root of the graph, we print the new event to the console, otherwise we pass it up.
        if (root.isEmpty) println(s"Received from event graph: $event") else context.parent ! event
      }
    }
  })

  val lhsChild: Option[ActorRef] = lhsQuery match {
    // `lhsQuery` is a subscription to a stream, so we subscribe to it.
    case lhsStream: Stream => publishers(lhsStream.name) ! Subscribe; None
    // `lhsQuery` is another join, so we create a child actor to perform that join.
    case lhsJoin: Join => Some(context.actorOf(Props(
      new JoinActor(lhsJoin, publishers, Some(root.getOrElse(self)))), s"${self.path.name}-lhs"))
  }

  val rhsChild: Option[ActorRef] = rhsQuery match {
    // Suppose `lhsQuery` and `rhsQuery` are a subscription to the same stream -- then, we won't subscribe again.
    case rhsStream: Stream => lhsQuery match {
      case lhsStream: Stream if lhsStream.name == rhsStream.name => None
      case _ => publishers(rhsStream.name) ! Subscribe; None
    }
    case join: Join => Some(context.actorOf(Props(
      new JoinActor(join, publishers, Some(root.getOrElse(self)))), s"${self.path.name}-rhs"))
  }

  // `receive` receives events and figures out whether to send them to the Esper engine as instance of `lhs` or `rhs`.
  override def receive: Receive = {
    case event: Event =>
      val senderName = sender.path.name
      val eventAsArray: Array[AnyRef] = getArrayOfValuesFrom(event)
      (lhsQuery, rhsQuery) match {
        // `lhsQuery` and `rhsQuery` are both subscriptions to the same stream, and `event` is from this stream.
        case (lhsStream: Stream, rhsStream: Stream)
          if lhsStream.name == rhsStream.name && lhsStream.name == senderName =>
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
