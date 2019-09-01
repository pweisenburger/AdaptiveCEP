package adaptivecep.distributed.centralized

import java.io.File

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed.operator.{ActiveOperator, Host, NodeHost, Operator}
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos._
import adaptivecep.privacy._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import akka.stream._
import com.typesafe.config.ConfigFactory
import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._
import argonaut._

object AppRunnerCentralized extends App {

  implicit val pc: PrivacyContext = NoPrivacyContext


  val file = new File("application.conf")
  val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
  var producers: Seq[Operator] = Seq.empty[Operator]
  var optimizeFor: String = "bandwidth"

  val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

  val query1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(2.seconds),
        slidingWindow(2.seconds))
      .where(_ < _)
      .dropElem1(
        latency < timespan(1.milliseconds) otherwise { nodeData => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!") })
      .selfJoin(
        tumblingWindow(1.instances),
        tumblingWindow(1.instances),
        frequency > ratio(3.instances, 5.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },
        frequency < ratio(12.instances, 15.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") })
      .and(stream[Float]("C"))
      .or(stream[String]("D"))

  val query2: Query4[Int, Int, Float, String] =
    stream[Int]("A")
      .and(stream[Int]("B"))
      .join(
        sequence(
          nStream[Float]("C") -> nStream[String]("D"),
          frequency > ratio(1.instances, 5.seconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/}),
        slidingWindow(3.seconds),
        slidingWindow(3.seconds),
        bandwidth > dataRate(40.mbPerSecond) otherwise { nodeData => },
        latency < timespan(100.milliseconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!")*/})

  val query3: Query2[Either[Int, Either[Float, String]], Either[Int, X]] =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(2.seconds),
        slidingWindow(2.seconds))
      .dropElem1()
      .selfJoin(
        tumblingWindow(1.instances),
        tumblingWindow(1.instances)
        /*frequency > ratio(3.instances, 5.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },*/
        /*frequency < ratio(12.instances, 15.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") }*/)
      .or(stream[Float]("C").or(stream[String]("D")),
        /*bandwidth > dataRate(70.mbPerSecond) otherwise { nodeData => },*/
        /*latency < timespan(200.milliseconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!")*/ },*/
        frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

  val query4: Query1[Either[Either[Int, Int], Either[Float, String]]] =
    stream[Int]("A")
      .or(
        stream[Int]("B"))
      /*frequency > ratio(3.instances, 5.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },*/
      /*frequency < ratio(12.instances, 15.seconds) otherwise { nodeData => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") }*/
      .or(stream[Float]("C").or(stream[String]("D")),
      /*bandwidth > dataRate(70.mbPerSecond) otherwise { nodeData => },*/
      /*latency < timespan(200.milliseconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!")*/ },*/
      frequency > ratio(3000.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

  val query5: Query1[Either[Int, Int]] =
    stream[Int, Int, Int, Int]("A")
      .or(
        stream[Int, Int, Int, Int]("B"))
      .dropElem1()
      .dropElem1()
      .dropElem1(frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/}
        /*latency < timespan(200.milliseconds) otherwise { (nodeData) => /*println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!")*/ }*/)


  val simpleQuery: Query1[Int] =
    stream[Int]("A").
      where(x => x % 2 == 0, frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

  val address1 = Address("akka.tcp", "ClusterSystem", "40.115.4.25", 8000)
  val address2 = Address("akka.tcp", "ClusterSystem", sys.env("HOST2"), 8000)
  val address3 = Address("akka.tcp", "ClusterSystem", sys.env("HOST3"), 8000)
  val address4 = Address("akka.tcp", "ClusterSystem", sys.env("HOST4"), 8000)

  val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
  val host2: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
  val host3: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
  val host4: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")


  val hosts: Set[ActorRef] = Set(host1, host2, host3, host4)

  hosts.foreach(host => host ! Hosts(hosts))

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")

  //  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event4(id, id, id, id))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
  //  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event4(id * 2, id * 2, id * 2, id * 2))).withDeploy(Deploy(scope = RemoteScope(address2))), "B")
  //  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))).withDeploy(Deploy(scope = RemoteScope(address3))), "C")
  //  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))).withDeploy(Deploy(scope = RemoteScope(address4))), "D")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA
    //    , "B" -> publisherB
    //    ,"C" -> publisherC
    //    ,"D" -> publisherD
  )

  val publisherHosts: Map[String, Host] = Map(
    "A" -> NodeHost(host1)
    //    , "B" -> NodeHost(host2)
    //    ,"C" -> NodeHost(host3)
    //    ,"D" -> NodeHost(host4)
  )

  hosts.foreach(host => host ! OptimizeFor(optimizeFor))

  Thread.sleep(5000)

  println("\n Publishes map created and hosts map\n")

  val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
    simpleQuery,
    publishers,
    publisherHosts,
    AverageFrequencyMonitorFactory(interval = 3000, logging = false),
    PathLatencyMonitorFactory(interval = 1000, logging = false),
    PathBandwidthMonitorFactory(interval = 1000, logging = false), NodeHost(host4), hosts, optimizeFor) ), "Placement")

  println("\n Calling Initialize query \n")

  placement ! InitializeQuery
  Thread.sleep(10000)
  placement ! Start

}

