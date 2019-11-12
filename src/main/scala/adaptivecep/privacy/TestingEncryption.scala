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


object TestingEncryption extends App {


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
    //    val address8 = Address("akka.tcp", "ClusterSystem", sys.env("HOST8"), 8000)

    ////deploy host actors
    val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")
    val host2: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address2))), "Host" + "2")
    val host3: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address3))), "Host" + "3")
    val host4: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address4))), "Host" + "4")
    //    val host5: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address5))), "Host" + "5")
    //    val host6: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address6))), "Host" + "6")
    //    val host7: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address7))), "Host" + "7")
    //    val host8: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address8))), "Host" + "7")

    //    val hosts: Set[ActorRef] = Set(host1, host2, host3, host4, host5, host6, host7)
    val hosts: Set[ActorRef] = Set(host1, host2, host3, host4)

    hosts.foreach(host => host ! Hosts(hosts))

    /////deploy publishers
    //    val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))), "A")
    //    val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id * 2))).withDeploy(Deploy(scope = RemoteScope(address2))), "B")
    val employeePublisher: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(Employee(id, "ahmad", id * 10)))).withDeploy(Deploy(scope = RemoteScope(address1))), "E")

    val employeePublishers: Map[String, ActorRef] = Map(
      "E" -> employeePublisher
    )

    val employeePublishersHosts: Map[String, Host] = Map(
      "E" -> NodeHost(host1)
    )


    val cryptoActor: ActorRef = actorSystem.actorOf(Props[CryptoServiceActor].withDeploy(Deploy(scope = RemoteScope(address1))), "CryptoService")

    val cryptoSvc = new CryptoServiceWrapper(cryptoActor)

    val interpret = new CEPRemoteInterpreter(cryptoSvc)



    //    val encQuery: Query1[EncInt] =
    //      stream[EncInt]("A").
    //        where(x => interpret(isEven(x)), frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})


//    val carEventPublisher: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(CarEvent(s"${id % 99}${(id + 2) % 99}${(id + 3) % 99}", id % 200)))).withDeploy(Deploy(scope = RemoteScope(address1))), "C")
//    val checkpointEventPublisher: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(CheckPointEvent(id, s"${id % 99}${(id + 2) % 99}${(id + 3) % 99}", id % 24, id % 60)))).withDeploy(Deploy(scope = RemoteScope(address2))), "K")
//
//
//    val carEventTransformer = EncDecTransformer(encryptCarEvent, decryptCarEvent)
//    val checkpointTransformer = EncDecTransformer(encryptCheckPointEvent, decryptCheckPointEvent)
//
//
//    val complexPublishers: Map[String, ActorRef] = Map(
//      "C" -> carEventPublisher
//      , "K" -> checkpointEventPublisher
//    )
//
//    val complexPublishersHosts: Map[String, Host] = Map(
//      "C" -> NodeHost(host1)
//      , "K" -> NodeHost(host2)
//    )

    //    val simplePublishers: Map[String, ActorRef] = Map(
    //      "A" -> publisherA
    //      , "B" -> publisherB
    //    )

    //    val simplePublisherHosts: Map[String, Host] = Map(
    //      "A" -> NodeHost(host1)
    //      ,"B" -> NodeHost(host2)
    //    )

    val optimizeFor = "bandwidth"
    hosts.foreach(host => host ! OptimizeFor(optimizeFor))

    Thread.sleep(5000)


//    val eventProcessorClient = EventProcessorClient("13.80.151.52", 60000)


    //    implicit val sgxPrivacyContext: PrivacyContext = SgxPrivacyContext(
    //      Set(TrustedHost(NodeHost(host1))), // Trusted hosts
    //      eventProcessorClient,
    //      Map("C" -> Event1Rule(carEventTransformer), "K" -> Event1Rule(checkpointTransformer))
    //      //      Map("A" -> Event1Rule(IntEventTransformer), "B" -> Event1Rule(IntEventTransformer))
    //    )

//    val simpleCarQuery: Query1[CheckPointEvent] = stream[CheckPointEvent]("K").
//      where(c => c.id > 1000, frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})
//

    /** *
      * this query is to be run with SGX Mode
      * uncomment the following query
      */
//    val carQuery: Query2[CarEvent, CheckPointEvent] =
//      stream[CarEvent]("C")
//        .join(
//          stream[CheckPointEvent]("K"),
//          slidingWindow(1.instances), slidingWindow(1.instances))
//        .where((car, checkpoint) =>
//          car.plateNumber == checkpoint.plateNumber && car.speed > 30
//          , frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/}
//        )

    val enc5000: EncInt = Await.result(cryptoSvc.encrypt(Comparable)(5000), Duration(5, TimeUnit.SECONDS))

    val employeeQuery: Query1[EmployeeEnc] =
      stream[EmployeeEnc]("E")
        .where(e => interpret(e.salary > enc5000))

    //    implicit val phePrivacyContext: PrivacyContext = PhePrivacyContext(
    //      cryptoSvc,
    //      Map("A" -> Event1Rule(IntPheSourceMapper))
    //    )

    val employeePheSourceMapper: Transformer = PheSourceTransformer(pheMapEmployee)
    implicit val employeePhePrivacyContext: PrivacyContext = PhePrivacyContext(
      cryptoSvc,
      Map("E" -> Event1Rule(employeePheSourceMapper))
    )


    //        implicit val empSgxPrivacyContext: PrivacyContext = SgxPrivacyContext(
    //          Set(TrustedHost(NodeHost(host1))), // Trusted hosts
    //          eventProcessorClient,
    //          Map("E" -> Event1Rule(empTransformer))
    //        )


    //    val employeeQuery: Query1[Employee] =
    //      stream[Employee]("E").
    //        join(stream[EntryEvent]("D"))
    //      where( e => e.salary > 5000,frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})
    //
    //      implicit val pc: PrivacyContext = NoPrivacyContext

    //    val normalQuery: Query2[Int, Int] =
    //      stream[Int]("A").
    //        join(stream[Int]("B"), slidingWindow(1.instances), slidingWindow(1.instances)).
    //        where((a, b) => a < b, frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => })
    //    val normalQuery: Query2[Int, Int] =
    //      stream[Int]("A").
    //        join(stream[Int]("B"), slidingWindow(1.instances), slidingWindow(1.instances)).
    //        where((a, b) => a < b, frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})


    //    val orQuery =
    //      stream[Int]("A").
    //        or(stream[String]("B")).
    //        where((x) => x.left.get > 3)

    //
    //    val simpleQuery: Query1[Int] =
    //      stream[Int]("A")
    //        .where(a => a < 3000,frequency > ratio(3500.instances, 1.seconds) otherwise { nodeData => /*println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!")*/})

    val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
      employeeQuery,
      employeePublishers,
      employeePublishersHosts,
      AverageFrequencyMonitorFactory(interval = 3000, logging = false),
      PathLatencyMonitorFactory(interval = 1000, logging = false),
      PathBandwidthMonitorFactory(interval = 1000, logging = false), NodeHost(host4),
      hosts, optimizeFor)), "Placement")

    //    val placement: ActorRef = actorSystem.actorOf(Props(PlacementActorCentralized(actorSystem,
    //      carQuery,
    //      complexPublishers,
    //      complexPublishersHosts,
    //      AverageFrequencyMonitorFactory(interval = 3000, logging = false),
    //      PathLatencyMonitorFactory(interval = 1000, logging = false),
    //      PathBandwidthMonitorFactory(interval = 1000, logging = false), NodeHost(host4),
    //      hosts, optimizeFor)), "Placement")

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
