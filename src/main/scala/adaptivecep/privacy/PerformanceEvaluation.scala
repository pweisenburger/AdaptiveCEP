package adaptivecep.privacy

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import adaptivecep.privacy.ConversionRules._
import adaptivecep.privacy.shared.Custom._
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
import adaptivecep.privacy.phe.{CEPRemoteInterpreter, CryptoServiceActor, CryptoServiceWrapper}
import adaptivecep.privacy.sgx.EventProcessorClient
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.{RemoteScope, WireFormats}
import com.typesafe.config.ConfigFactory
import crypto.dsl._
import adaptivecep.privacy.shared.Custom._
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration.Duration


object PerformanceEvaluation extends App {


  override def main(args: Array[String]): Unit = {

    //Joining the previously configured cluster
    val file = new File("application.conf")
    val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
    val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

    ///declare remote nodes addresses
    val address1 = Address("akka.tcp", "ClusterSystem", sys.env("HOST1"), 8000)
    val address2 = Address("akka.tcp", "ClusterSystem", sys.env("HOST2"), 8000)
    val address3 = Address("akka.tcp", "ClusterSystem", sys.env("HOST3"), 8000)
    val address4 = Address("akka.tcp", "ClusterSystem", sys.env("HOST4"), 8000)
    //    val address5 = Address("akka.tcp", "ClusterSystem", sys.env("HOST5"), 8000)
    //    val address6 = Address("akka.tcp", "ClusterSystem", sys.env("HOST6"), 8000)
    //    val address7 = Address("akka.tcp", "ClusterSystem", sys.env("HOST7"), 8000)

    ////deploy host actors
    val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
    val host2: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
    val host3: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
    val host4: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")
    //    val host5: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "5")
    //    val host6: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address6))), "Host" + "6")
    //    val host7: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address7))), "Host" + "7")

    //    val hosts: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host6, host7)
    val hosts: Set[ActorRef] = Set(host1, host2, host3, host4)

    hosts.foreach(host => host ! Hosts(hosts))

    /////deploy publishers
    val publisherA: ActorRef = actorSystem.actorOf(Props(EvaluationPublisher(id => Event1(MeasureEvent(java.util.UUID.randomUUID.toString, id)))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
    val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))).withDeploy(Deploy(scope = RemoteScope(address2))), "B")
    //
    val cryptoActor: ActorRef = actorSystem.actorOf(Props[CryptoServiceActor].withDeploy(Deploy(scope = RemoteScope(address1))), "CryptoService")

    val cryptoSvc = new CryptoServiceWrapper(cryptoActor)

    val interpret = new CEPRemoteInterpreter(cryptoSvc)

    val encOne: EncInt = Await.result(cryptoSvc.encrypt(Comparable)(1), Duration(5, TimeUnit.SECONDS))
    val encTwo: EncInt = Await.result(cryptoSvc.encrypt(Comparable)(2), Duration(5, TimeUnit.SECONDS))

    val query: Query2[MeasureEventEncPhe, EncInt] =
      stream[MeasureEventEncPhe]("A").and(stream[EncInt]("B"))
        .where((x, y) => interpret(interpret(interpret(x.data * encTwo) + encOne) > y), frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

    //    val query: Query2[MeasureEvent, Int] =
    //      stream[MeasureEvent]("A").
    //        and(stream[Int]("B"))
    //        .where((x, y) => x.data * 2 + 1 > y, frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})


    val publishers: Map[String, ActorRef] = Map(
      "A" -> publisherA,
      "B" -> publisherB
    )

    val publishersHosts: Map[String, Host] = Map(
      "A" -> NodeHost(host1),
      "B" -> NodeHost(host2)
    )

    val optimizeFor = "bandwidth"
    hosts.foreach(host => host ! OptimizeFor(optimizeFor))

    Thread.sleep(5000)

    /** *
      * Normal operations with no privacy what so ever
      */
    //    implicit val pc: PrivacyContext = NoPrivacyContext


    /** *
      * SGX enabled privacy
      */
    //
    //    val eventProcessorClient = EventProcessorClient("13.80.151.52", 60000)
    //    val measureEventTransformer = EncDecTransformer(encryptMeasureEvent, decryptMeasureEvent)
    //    implicit val sgxPrivacyContext: PrivacyContext = SgxPrivacyContext(
    //      Set(TrustedHost(NodeHost(host1))), // Trusted hosts
    //      eventProcessorClient,
    //      Map("A" -> Event1Rule(measureEventTransformer), "B" -> Event1Rule(IntEventTransformer))
    //    )


    /** *
      * PHE enabled privacy
      */

    val measureEventSourceMapper = PheSourceTransformer(pheMapMeasureEvent)
    implicit val phePrivacyContext: PrivacyContext = PhePrivacyContext(
      cryptoSvc,
      Map("A" -> Event1Rule(measureEventSourceMapper), "B" -> Event1Rule(IntPheSourceMapper))
    )


    val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
      query,
      publishers,
      publishersHosts,
      AverageFrequencyMonitorFactory(interval = 3000, logging = false),
      PathLatencyMonitorFactory(interval = 1000, logging = false),
      PathBandwidthMonitorFactory(interval = 1000, logging = false), NodeHost(host4),
      hosts, optimizeFor)), "Placement")

    println("\n Calling Initialize query \n")

    placement ! InitializeQuery
    Thread.sleep(10000)
    placement ! Start


  }

}
