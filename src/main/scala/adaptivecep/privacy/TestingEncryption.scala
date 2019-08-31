package adaptivecep.privacy

import java.io.File

import adaptivecep.privacy.ConversionRules._
import akka.pattern.ask
import crypto._
import adaptivecep.publishers._
import crypto.cipher._
import crypto.dsl.Implicits._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.distributed.centralized.{HostActorCentralized, PlacementActorCentralized}
import adaptivecep.distributed.operator.{Host, NodeHost, TrustedHost}
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.qos.{AverageFrequencyMonitorFactory, PathBandwidthMonitorFactory, PathLatencyMonitorFactory}
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.encryption.{CryptoAES, Encryption}
import adaptivecep.privacy.sgx.EventProcessorClient
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import crypto.dsl._

import scala.concurrent.Await

object TestingEncryption extends App {

  def test(in: Either[Int,String]) = {

  }

  override def main(args: Array[String]): Unit = {

    val file = new File("application.conf")
    val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
    val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

    val address1 = Address("akka.tcp", "ClusterSystem", "40.115.4.25", 8000)
    val address2 = Address("akka.tcp", "ClusterSystem", sys.env("HOST2"), 8000)
    val address3 = Address("akka.tcp", "ClusterSystem", sys.env("HOST3"), 8000)
    val address4 = Address("akka.tcp", "ClusterSystem", sys.env("HOST4"), 8000)

    val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
    val host2: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
    val host3: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
    val host4: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")
    //  val host5: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "5")

    val hosts: Set[ActorRef] = Set(host1, host2, host3, host4)

    hosts.foreach(host => host ! Hosts(hosts))

    //    val cryptoActor: ActorRef = actorSystem.actorOf(Props[CryptoServiceActor].withDeploy(Deploy(scope = RemoteScope(address1))), "CryptoService")

    //    val publisher: ActorRef = actorSystem.actorOf(Props(EncryptedPublisher(cryptoActor, id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
    val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
    val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))), "B")

    //    val cryptoSvc = new CryptoServiceWrapper(cryptoActor)
    //    val interpret = new CEPRemoteInterpreter(cryptoActor)

    val publishers: Map[String, ActorRef] = Map(
      "A" -> publisherA
      , "B" -> publisherB
      //    ,"C" -> publisherC
      //    ,"D" -> publisherD
    )

    val publisherHosts: Map[String, Host] = Map(
      "A" -> NodeHost(host1)
      , "B" -> NodeHost(host2)
      //    ,"C" -> NodeHost(host3)
      //    ,"D" -> NodeHost(host4)
    )

    val optimizeFor = "bandwidth"
    hosts.foreach(host => host ! OptimizeFor(optimizeFor))

    Thread.sleep(5000)

//    val eventProcessorClient = EventProcessorClient("13.80.151.52", 60000)
//    val remoteObject = eventProcessorClient.lookupObject()

//    implicit val sgxPrivacyContext: PrivacyContext = SgxPrivacyContext(
//      Set(TrustedHost(NodeHost(host1)), TrustedHost(NodeHost(host4))), // Trusted hosts
//      remoteObject, // a reference to the remote sgx object? do we need it?
//      Map("A" -> Event1Rule(IntEventTransformer), "B" -> Event1Rule(NoTransformer))
//
//    )

      implicit val pc: PrivacyContext = NoPrivacyContext

    val normalQuery: Query2[Int,Int] =
      stream[Int]("A").
        join(stream[Int]("B"), slidingWindow(1.instances), slidingWindow(1.instances) ).
        where((a,b) => a > b, frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

//    val testQ = stream[Int]("A").or(stream[Double,String]("B"))
//      .where()


    val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
      normalQuery,
      publishers,
      publisherHosts,
      AverageFrequencyMonitorFactory(interval = 3000, logging = false),
      PathLatencyMonitorFactory(interval = 1000, logging = false),
      PathBandwidthMonitorFactory(interval = 1000, logging = false), NodeHost(host4),
      hosts, optimizeFor)), "Placement")

    println("\n Calling Initialize query \n")

    placement ! InitializeQuery
    Thread.sleep(10000)
    placement ! Start

    //    val oneEnc = cryptoSvc.encryptInt(Comparable, 1)
    //    val twoEnc = cryptoSvc.encryptInt(Comparable, 2)
    //    cryptoSvc.decryptAndPrint(oneEnc)
    //    cryptoSvc.decryptAndPrint(twoEnc)
    //    val result = interpret(oneEnc < twoEnc)
    //    println(result)

  }

}
