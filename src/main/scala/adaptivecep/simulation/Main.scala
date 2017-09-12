package adaptivecep.simulation

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Props}

object Main extends App {

  val actorSystem: ActorSystem = ActorSystem()

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))), "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))), "B")

   val query: Query2[Int, Int] =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(2.seconds),
        slidingWindow(2.seconds))
      .where(_ < _)
      .removeElement1()
      .selfJoin(
        tumblingWindow(1.instances),
        tumblingWindow(1.instances),
        frequency > ratio( 3.instances,  5.seconds) otherwise { (name) => println(s"PROBLEM:\tNode `$name` emits too few events!") },
        frequency < ratio(12.instances, 15.seconds) otherwise { (name) => println(s"PROBLEM:\tNode `$name` emits too many events!") },
        latency   < timespan(1.milliseconds)        otherwise { (name) => println(s"PROBLEM:\tEvents reach node `$name` too slow!") })

  val graph: ActorRef = actorSystem.actorOf(Props(SelfJoinNode(
    query.asInstanceOf[SelfJoinQuery],
    Map("A" -> publisherA, "B" -> publisherB),
    AveragedFrequencyMonitorFactory(interval = 15, logging = true),
    PathLatencyMonitorFactory(interval = 10, logging = true),
    Some(event => println(s"COMPLEX EVENT:\t$event")))),
    "selfjoin")

}
