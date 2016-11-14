package simple_join_with_akka_and_esper

import scala.reflect.{ClassTag, classTag}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.espertech.esper.client._

case class Stream2[A <: AnyRef, B <: AnyRef](t: (A, B))(implicit val aCt: ClassTag[A], val bCt: ClassTag[B])
case class Stream4[A <: AnyRef, B <: AnyRef, C <: AnyRef, D <: AnyRef](t: (A, B, C, D))(implicit val aCt: ClassTag[A], val bCt: ClassTag[B], val cCt: ClassTag[C], val dCt: ClassTag[D])

class ReceiverActor extends Actor {

  def receive = {
    case s: Stream4[_, _, _, _] =>
      if (s.aCt == classTag[Integer] && s.bCt == classTag[String] && s.cCt == classTag[String] && s.dCt == classTag[Integer]) {
        val genericS: Stream4[Integer, String, String, Integer] = s.asInstanceOf[Stream4[Integer, String, String, Integer]]
        println(s"Result of join: $genericS")
      } else {
        println("Got a Stream4, but not with the right type arguments.")
      }
    case _ =>
      println("Got no Stream4 at all.")
  }

}

class JoinActor(receiverActor: ActorRef) extends Actor {

  val configuration = new Configuration

  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  val lhsNames = Array[String]("P0", "P1")
  val lhsTypes = Array[AnyRef](classTag[Integer].runtimeClass, classTag[String].runtimeClass)
  val rhsNames = Array[String]("P0", "P1")
  val rhsTypes = Array[AnyRef](classTag[String].runtimeClass, classTag[Integer].runtimeClass)
  configuration.addEventType("lhs", lhsNames, lhsTypes)
  configuration.addEventType("rhs", rhsNames, rhsTypes)

  val eplStatement = administrator.createEPL(
    "select * from lhs.win:length_batch(1) as l, rhs.win:length_batch(1) as r")

  eplStatement.addListener(new UpdateListener {
    def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
      val lP0: Integer = newEvents(0).get("l").asInstanceOf[Array[AnyRef]](0).asInstanceOf[Integer]
      val lP1: String = newEvents(0).get("l").asInstanceOf[Array[AnyRef]](1).asInstanceOf[String]
      val rP0: String = newEvents(0).get("r").asInstanceOf[Array[AnyRef]](0).asInstanceOf[String]
      val rP1: Integer = newEvents(0).get("r").asInstanceOf[Array[AnyRef]](1).asInstanceOf[Integer]
      receiverActor ! Stream4[Integer, String, String, Integer](lP0, lP1, rP0, rP1)
    }
  })

  def stream2ToArray[A <: AnyRef, B <: AnyRef](s: Stream2[A, B]): Array[AnyRef] =
    Array[AnyRef](s.t._1, s.t._2)

  def receive = {
    case s: Stream2[_, _] =>
      if (s.aCt == classTag[Integer] && s.bCt == classTag[String]) {
        val genericS: Stream2[Integer, String] = s.asInstanceOf[Stream2[Integer, String]]
        runtime.sendEvent(stream2ToArray(genericS), "lhs")
      } else if (s.aCt == classTag[String] && s.bCt == classTag[Integer]) {
        val genericS: Stream2[String, Integer] = s.asInstanceOf[Stream2[String, Integer]]
        runtime.sendEvent(stream2ToArray(genericS), "rhs")
      }
  }

}

object Working extends App {

  val actorSystem = ActorSystem()
  val receiverActor = actorSystem.actorOf(Props(new ReceiverActor))
  val joinActor = actorSystem.actorOf(Props(new JoinActor(receiverActor)))

  joinActor ! Stream2[Integer, String](42, "42")
  joinActor ! Stream2[String, Integer]("13", 13)

}
