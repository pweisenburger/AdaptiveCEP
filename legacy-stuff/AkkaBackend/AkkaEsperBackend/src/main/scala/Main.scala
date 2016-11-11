import akka.actor.{Actor, ActorSystem, Props}
import backend.{EventGraph, RandomEventPublisher}
import middleend._

import scala.beans.BeanProperty

object Main extends App {

  // Akka boilerplate
  val actorSystem = ActorSystem()

  // Primitive types
  case class A(@BeanProperty id: Int)
  case class B(@BeanProperty id: Int)
  case class C(@BeanProperty id: Int)
  case class D(@BeanProperty id: Int)

  val primitiveTypes = Map("A" -> classOf[A], "B" -> classOf[B], "C" -> classOf[C], "D" -> classOf[D])

  // Publishers randomly emitting instances of the primitive types above
  val aPublisher = actorSystem.actorOf(Props(RandomEventPublisher(id => A(id))), "aPublisher")
  val bPublisher = actorSystem.actorOf(Props(RandomEventPublisher(id => B(id))), "bPublisher")
  val cPublisher = actorSystem.actorOf(Props(RandomEventPublisher(id => C(id))), "cPublisher")
  val dPublisher = actorSystem.actorOf(Props(RandomEventPublisher(id => D(id))), "dPublisher")

  val primitivePublishers = Map("A" -> aPublisher, "B" -> bPublisher, "C" -> cPublisher, "D" -> dPublisher)

  // Query defining the complex event we want to detect
  val query =
    OrQuery(
      AndQuery(
        SequenceQuery(List(PrimitiveQuery("A"), PrimitiveQuery("B"), PrimitiveQuery("A"))),
        SequenceQuery(List(PrimitiveQuery("B"), PrimitiveQuery("C")))),
      PrimitiveQuery("D"))



  // Receiver actor handling detected complex events
  val receiverActor = actorSystem.actorOf(Props(new Actor {
    def receive = {
      case instance: EventInstance => println(s"\nComplex event detected:\t\t$instance\n")
    }
  }))

  // Event graph that subscribes to primitive sources, detects complex events, and reports them to receiver
  val eventGraph = actorSystem.actorOf(Props(new EventGraph(
    query = query,
    primitiveTypes = primitiveTypes,
    primitivePublishers = primitivePublishers,
    rootReceiver = Some(receiverActor))),
    name = "q0")

}
