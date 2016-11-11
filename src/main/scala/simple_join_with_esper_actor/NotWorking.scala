package simple_join_with_esper_actor

import akka.actor.{Actor, ActorSystem, Props}
import com.espertech.esper.client.{EventBean, UpdateListener}
import stream_representation.StreamRepresentation._

object NotWorking extends App {

  val actorSystem = ActorSystem()

  val receiverActor = actorSystem.actorOf(Props(new Actor { def receive = {
    // This will throw a warning, as type arguments will not be enforced...
    case stream4: Stream4[Int, String, Int, Char] => println(stream4)
    case something => sys.error(s"`receiverActor` received something that is no `Stream4`:\n$something")
  }}))

  val esperActor = actorSystem.actorOf(Props(new EsperActor(
    eventTypes = Map("StreamX" -> classOf[Stream2[Int, String]], "StreamY" -> classOf[Stream2[Int, Char]]),
    eplString = "select * from StreamX.win:length_batch(1) as x, StreamY.win:length_batch(1) as y",
    updateListener = new UpdateListener {
      def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
        val x = newEvents(0).get("x").asInstanceOf[Stream2[Int, String]]
        val y = newEvents(0).get("y").asInstanceOf[Stream2[Int, Char]]
        val stream4: Stream4[Int, String, Int, Char] = join22(x, y)
        receiverActor ! stream4
      }
    }
  )))

  println("Started!")
  esperActor ! Stream2[Int, String](42, "Foo")
  esperActor ! Stream2[Int, Char](13, 'c')

}
