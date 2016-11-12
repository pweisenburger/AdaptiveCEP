package simple_join_with_akka_and_esper

import akka.actor.{Actor, ActorSystem, Props}
import com.espertech.esper.client.{EventBean, UpdateListener}
import stream_representation.StreamRepresentation._

object KindOfWorking extends App {

  val actorSystem = ActorSystem()

  val receiverActor = actorSystem.actorOf(Props(new Actor { def receive = {
    // This will throw a warning, as type arguments will not be enforced...
    case stream4: Stream4[Int, Int, String, String] => println(stream4)
    case something => sys.error(s"`receiverActor` received something that is no `Stream4`:\n$something")
  }}))

  val esperActor = actorSystem.actorOf(Props(new EsperActor(
    eventTypes = Map("StreamX" -> classOf[Stream1[Int]], "StreamY" -> classOf[Stream3[Int, String, String]]),
    eplString = "select * from StreamX.win:length_batch(1) as x, StreamY.win:length_batch(1) as y",
    updateListener = new UpdateListener {
      def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
        val x = newEvents(0).get("x").asInstanceOf[Stream1[Int]]
        val y = newEvents(0).get("y").asInstanceOf[Stream3[Int, String, String]]
        val stream4: Stream4[Int, Int, String, String] = join13(x, y)
        receiverActor ! stream4
      }
    }
  )))

  println("Started!")
  esperActor ! Stream1[Int](42)
  esperActor ! Stream3[Int, String, String](13, "Foo", "Bar")

}
