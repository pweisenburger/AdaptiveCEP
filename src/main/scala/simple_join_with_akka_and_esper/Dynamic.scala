package simple_join_with_akka_and_esper

import scala.reflect.ClassTag
import akka.actor.{Actor, ActorSystem, Props}
import com.espertech.esper.client._
import EventQuery._
import EventInstance._

object EventQuery {

  sealed trait EventQuery
  case class Event1Query[A](implicit val ctA: ClassTag[A]) extends EventQuery
  case class Event2Query[A, B](implicit val ctA: ClassTag[A], val ctB: ClassTag[B]) extends EventQuery
  case class Event3Query[A, B, C](implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C]) extends EventQuery
  case class Event4Query[A, B, C, D](implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D]) extends EventQuery

  def getArrayOfNamesFrom(eventQuery: EventQuery): Array[String] = eventQuery match {
    case _: Event1Query[_] => Array("P0")
    case _: Event2Query[_, _] => Array("P0", "P1")
    case _: Event3Query[_, _, _] => Array("P0", "P1", "P2")
    case _: Event4Query[_, _, _, _] => Array("P0", "P1", "P2", "P3")
  }

  def getArrayOfClassesFrom(eventQuery: EventQuery): Array[java.lang.Class[_]] = eventQuery match {
    case q: Event1Query[_] => Array(q.ctA.runtimeClass)
    case q: Event2Query[_, _] => Array(q.ctA.runtimeClass, q.ctB.runtimeClass)
    case q: Event3Query[_, _, _] => Array(q.ctA.runtimeClass, q.ctB.runtimeClass, q.ctC.runtimeClass)
    case q: Event4Query[_, _, _, _] => Array(q.ctA.runtimeClass, q.ctB.runtimeClass, q.ctC.runtimeClass, q.ctD.runtimeClass)
  }

}

object EventInstance {

  sealed trait EventInstance
  case class Event1Instance[A](t: (A))(implicit val ctA: ClassTag[A]) extends EventInstance
  case class Event2Instance[A, B](t: (A, B))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B]) extends EventInstance
  case class Event3Instance[A, B, C](t: (A, B, C))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C]) extends EventInstance
  case class Event4Instance[A, B, C, D](t: (A, B, C, D))(implicit val ctA: ClassTag[A], val ctB: ClassTag[B], val ctC: ClassTag[C], val ctD: ClassTag[D]) extends EventInstance

  def getArrayOfValuesFrom(eventInstance: EventInstance): Array[AnyRef] = eventInstance match {
    case i: Event1Instance[_] => Array(i.t.asInstanceOf[AnyRef])
    case i: Event2Instance[_, _] => Array(i.t._1.asInstanceOf[AnyRef], i.t._2.asInstanceOf[AnyRef])
    case i: Event3Instance[_, _, _] => Array(i.t._1.asInstanceOf[AnyRef], i.t._2.asInstanceOf[AnyRef], i.t._3.asInstanceOf[AnyRef])
    case i: Event4Instance[_, _, _, _] => Array(i.t._1.asInstanceOf[AnyRef], i.t._2.asInstanceOf[AnyRef], i.t._3.asInstanceOf[AnyRef], i.t._4.asInstanceOf[AnyRef])
  }

  def getEventInstanceFrom(values: Array[AnyRef], classes: Array[java.lang.Class[_]]): EventInstance = {
    require(values.length == classes.length)
    values.length match {
      case 1 => Event1Instance(classes(0).cast(values(0)))
      case 2 => Event2Instance(classes(0).cast(values(0)), classes(1).cast(values(1)))
      case 3 => Event3Instance(classes(0).cast(values(0)), classes(1).cast(values(1)), classes(2).cast(values(2)))
      case 4 => Event4Instance(classes(0).cast(values(0)), classes(1).cast(values(1)), classes(2).cast(values(2)), classes(3).cast(values(3)))
    }
  }

}

class JoinActor(lhsQuery: EventQuery, rhsQuery: EventQuery) extends Actor {

  val configuration = new Configuration

  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  configuration.addEventType("lhs", getArrayOfNamesFrom(lhsQuery), getArrayOfClassesFrom(lhsQuery).asInstanceOf[Array[AnyRef]])
  configuration.addEventType("rhs", getArrayOfNamesFrom(rhsQuery), getArrayOfClassesFrom(rhsQuery).asInstanceOf[Array[AnyRef]])

  val eplStatement = administrator.createEPL(
    "select * from lhs.win:length_batch(1) as lhs, rhs.win:length_batch(1) as rhs")

  eplStatement.addListener(new UpdateListener {
    override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]): Unit = {
      for (nrOfNewEvent <- newEvents.indices) {
        val lhsValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("lhs").asInstanceOf[Array[AnyRef]]
        val rhsValues: Array[AnyRef] = newEvents(nrOfNewEvent).get("rhs").asInstanceOf[Array[AnyRef]]
        val lhsAndRhsValues: Array[AnyRef] = lhsValues ++ rhsValues
        val lhsAndRhsClasses: Array[java.lang.Class[_]] = getArrayOfClassesFrom(lhsQuery) ++ getArrayOfClassesFrom(rhsQuery)
        println(s"Goooooot a result: ${getEventInstanceFrom(lhsAndRhsValues, lhsAndRhsClasses)}!")
      }
    }
  })

  override def receive: Receive = {
    case i: EventInstance =>
      if (sender == context.child(s"${self.path.name}-lhs").getOrElse(println("Panic! This should never happen!"))) {
        runtime.sendEvent(getArrayOfValuesFrom(i), "lhs")
      } else if (sender == context.child(s"${self.path.name}-rhs").getOrElse(println("Panic! This should never happen!"))) {
        runtime.sendEvent(getArrayOfValuesFrom(i), "rhs")
      }
      else
        println("Panic! This should never happen!")
    case _ =>
      println("Panic! This should never happen!")
  }

  val lhsActor = context.actorOf(Props(new Actor {
    override def receive: Receive = { case _ => }
    context.parent ! Event2Instance[Integer, String](42, "42")
  }), s"${self.path.name}-lhs")

  val rhsActor = context.actorOf(Props(new Actor {
    override def receive: Receive = { case _ => }
    context.parent ! Event2Instance[String, Integer]("13", 13)
  }), s"${self.path.name}-rhs")

}

object Main extends App {

  println("Starting...")
  val actorSystem = ActorSystem()
  val joinActor = actorSystem.actorOf(Props(new JoinActor(Event2Query[Integer, String], Event2Query[String, Integer])), "join")

}
