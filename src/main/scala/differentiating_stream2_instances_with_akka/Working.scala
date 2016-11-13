package differentiating_stream2_instances_with_akka

import scala.reflect.{classTag, ClassTag}
import akka.actor.{Actor, ActorSystem, Props}

case class Stream[A, B](t: (A, B))(implicit val ctForA: ClassTag[A], val ctForB: ClassTag[B])

object Working extends App {

  val actorSystem = ActorSystem()

  val actor = actorSystem.actorOf(Props(new Actor {
    def receive = {
      case s: Stream[_, _] =>
        if (s.ctForA == classTag[Int] && s.ctForB == classTag[String])
          println("Got a Stream[Int, String]!")
        else if (s.ctForA == classTag[String] && s.ctForB == classTag[Int])
          println("Got a Stream[String, Int]!")
        else
          println("Got some other Stream!")
    }
  }))

  actor ! Stream[String, Int]("Hi!", 42)

}
