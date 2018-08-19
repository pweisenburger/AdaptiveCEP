package adaptivecep.simulation

import adaptivecep.data.Events._
import adaptivecep.data.Queries.HListQuery
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.factory._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Props}
import shapeless.{::, HNil, Nat}

object Main extends App {

  val actorSystem: ActorSystem = ActorSystem()

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))), "D")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC,
    "D" -> publisherD)

  val query1 = //: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
    stream[Int::HNil]("A")
    .join(
      stream[Int::HNil]("B"),
      slidingWindow(2.seconds),
      slidingWindow(2.seconds))
    .where(_ < _)
    .drop(Nat._1, latency < timespan(1.milliseconds) otherwise { (nodeData) => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!") })
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      frequency > ratio( 3.instances,  5.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },
      frequency < ratio(12.instances, 15.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") })
    .and(stream[Float::HNil]("C"))
    .or(stream[String::HNil]("D"))

  val query2 = // : HListQuery[Int::Int::Float::String::HNil] =
    stream[Int::HNil]("A")
    .and(stream[Int::HNil]("B"))
    .join(
      sequence(
        nStream[Float::HNil]("C") -> nStream[String::HNil]("D"),
        frequency > ratio(1.instances, 5.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") }),
      slidingWindow(3.seconds),
      slidingWindow(3.seconds),
      latency < timespan(1.milliseconds) otherwise { (nodeData) => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!") })


  val graph: ActorRef = GraphFactory.create[Int, Int, Float, String](
    actorSystem =             actorSystem,
    // need to typecast because otherwise the scala compiler cannot infer the correct type
    // TODO is there a fix?
    query =                   query2.asInstanceOf[HListQuery[Int::Int::Float::String::HNil]], // Alternatively: `query2`
    publishers =              publishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory  (interval = 15, logging = true),
    latencyMonitorFactory =   PathLatencyMonitorFactory       (interval =  5, logging = true),
    createdCallback =         () => println("STATUS:\t\tGraph has been created."))(
    eventCallback =           {
      // Callback for `query1`:
      // case (Left(i1), Left(i2), Left(f)) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
      // case (Right(s), _, _)              => println(s"COMPLEX EVENT:\tEvent1($s)")
      // Callback for `query2`:
      case (i1, i2, f, s) => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f, $s)")
      // This is necessary to avoid warnings about non-exhaustive `match`:
      case _ =>
    })

}
