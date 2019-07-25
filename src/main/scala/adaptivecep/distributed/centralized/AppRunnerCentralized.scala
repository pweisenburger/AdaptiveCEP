package adaptivecep.distributed.centralized

import java.io.File

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed.operator.{ActiveOperator, Host, NodeHost, Operator}
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos._
import adaptivecep.publishers._
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import akka.stream._
import com.typesafe.config.ConfigFactory


import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._

sealed trait Student

case class Student1[A](id: A, name: String) extends Student {
}

object AppRunnerCentralized extends App {

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

  def isBoolean(s: Student) = true

  lazy val keyRing = KeyRing.create
  val interpret = new LocalInterpreter(keyRing)

  val studentsQuery: Query1[Student] =
    stream[Student]("A").
      where(x => isBoolean(x), frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})
  //
//  val encQuery: Query1[EncInt] =
//    stream[EncInt]("A").
//      where(x => interpret(isEven(x)), frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

  //  val v = Common.encrypt(Comparable,keyRing)(BigInt(1))


  val address1 = Address("akka.tcp", "ClusterSystem", "40.115.4.25", 8000)
  val address2 = Address("akka.tcp", "ClusterSystem", sys.env("HOST2"), 8000)
  val address3 = Address("akka.tcp", "ClusterSystem", sys.env("HOST3"), 8000)
  val address4 = Address("akka.tcp", "ClusterSystem", sys.env("HOST4"), 8000)
  //  val address5 = Address("akka.tcp", "ClusterSystem", sys.env("HOST5"), 8000)
  //  val address6 = Address("akka.tcp", "ClusterSystem", sys.env("HOST6"), 8000)
  //  val address7 = Address("akka.tcp", "ClusterSystem", sys.env("HOST7"), 8000)
  //  val address8 = Address("akka.tcp", "ClusterSystem", sys.env("HOST8"), 8000)
  //  val address9 = Address("akka.tcp", "ClusterSystem", sys.env("HOST9"), 8000)
  //  val address10 = Address("akka.tcp", "ClusterSystem", sys.env("HOST10"), 8000)
  //  val address11 = Address("akka.tcp", "ClusterSystem", sys.env("HOST11"), 8000)
  //  val address12 = Address("akka.tcp", "ClusterSystem", sys.env("HOST12"), 8000)
  //  val address13 = Address("akka.tcp", "ClusterSystem", sys.env("HOST13"), 8000)
  //  val address14 = Address("akka.tcp", "ClusterSystem", sys.env("HOST14"), 8000)
  //  val address15 = Address("akka.tcp", "ClusterSystem", sys.env("HOST15"), 8000)
  //  val address16 = Address("akka.tcp", "ClusterSystem", sys.env("HOST16"), 8000)
  //  val address17 = Address("akka.tcp", "ClusterSystem", sys.env("HOST17"), 8000)
  //  val address18 = Address("akka.tcp", "ClusterSystem", sys.env("HOST18"), 8000)
  //  val address19 = Address("akka.tcp", "ClusterSystem", sys.env("HOST19"), 8000)
  //  val address20 = Address("akka.tcp", "ClusterSystem", sys.env("HOST20"), 8000)

  //LOCAL Setup
  /*
  val address1 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2551)
  val address2 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2552)
  val address3 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2553)
  val address4 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2554)
  val address5 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2555)
  val address6 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2556)
  val address7 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2557)
  val address8 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2558)
  val address9 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2559)
  val address10 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2560)
  val address11 = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2561)
  */


  val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
  val host2: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
  val host3: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
  val host4: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")
  //  val host5: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "5")
  //  val host6: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address6))), "Host" + "6")
  //  val host7: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address7))), "Host" + "7")
  //  val host8: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address8))), "Host" + "8")
  //  val host9: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address9))), "Host" + "9")
  //  val host10: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address10))), "Host" + "10")
  //  val host11: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address11))), "Host" + "11")
  //  val host12: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address12))), "Host" + "12")
  //  val host13: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address13))), "Host" + "13")
  //  val host14: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address14))), "Host" + "14")
  //  val host15: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address15))), "Host" + "15")
  //  val host16: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address16))), "Host" + "16")
  //  val host17: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address17))), "Host" + "17")
  //  val host18: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address18))), "Host" + "18")
  //  val host19: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address19))), "Host" + "19")
  //  val host20: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address20))), "Host" + "20")

  //  val hosts: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host6, host7, host8, host9, host10, host11,
  //    host12, host13, host14, host15, host16, host17, host18, host19, host20)

  val hosts: Set[ActorRef] = Set(host1, host2, host3, host4)

  hosts.foreach(host => host ! Hosts(hosts))

  //  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
//  val publisherAEnc: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(Common.encrypt(Comparable, keyRing)(BigInt(id))))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
    val studentsPublisher: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1( Student1(id,"ahmad") ))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")

  //  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event4(id, id, id, id))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
  //  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event4(id * 2, id * 2, id * 2, id * 2))).withDeploy(Deploy(scope = RemoteScope(address2))), "B")
  //  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))).withDeploy(Deploy(scope = RemoteScope(address3))), "C")
  //  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))).withDeploy(Deploy(scope = RemoteScope(address4))), "D")

  val operatorA = ActiveOperator(null, Seq.empty[Operator])
  //  val operatorB = ActiveOperator(null, Seq.empty[Operator])
  //  val operatorC = ActiveOperator(null, Seq.empty[Operator])
  //  val operatorD = ActiveOperator(null, Seq.empty[Operator])
  //

  val publishers: Map[String, ActorRef] = Map(
    "A" -> studentsPublisher
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

  val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
    studentsQuery,
    publishers, publisherHosts,
    AverageFrequencyMonitorFactory(interval = 3000, logging = false),
    PathLatencyMonitorFactory(interval = 1000, logging = false),
    PathBandwidthMonitorFactory(interval = 1000, logging = false), NodeHost(host4), hosts, optimizeFor)), "Placement")

  placement ! InitializeQuery
  Thread.sleep(10000)
  placement ! Start

}

