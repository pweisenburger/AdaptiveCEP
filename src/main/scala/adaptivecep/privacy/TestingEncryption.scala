package adaptivecep.privacy

import java.io.File

import akka.pattern.ask

import scala.concurrent.duration._
import crypto._
import crypto.cipher._
import crypto.dsl.Implicits._
import adaptivecep.data.Events.{DecryptIntAndPrintRequest, EncryptIntRequest}
import adaptivecep.distributed.centralized.HostActorCentralized
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await

object TestingEncryption extends App {
  override def main(args: Array[String]): Unit = {

    val file = new File("application.conf")
    val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
    val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

    val address1 = Address("akka.tcp", "ClusterSystem", "40.115.4.25", 8000)
    val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")

    val cryptoActor: ActorRef = actorSystem.actorOf(Props[CryptoServiceActor].withDeploy(Deploy(scope = RemoteScope(address1))), "Crypto")

    val cryptoSvc = CryptoServiceWrapper(cryptoActor)
    val interpreter = CEPRemoteInterpreter(cryptoActor)

    val oneEnc = cryptoSvc.encryptInt(Comparable, 1)
    val twoEnc = cryptoSvc.encryptInt(Comparable, 2)

    cryptoSvc.decryptAndPrint(oneEnc)
    cryptoSvc.decryptAndPrint(twoEnc)
    val result = interpreter.interpret(oneEnc + twoEnc)
    cryptoSvc.decryptAndPrint(result)


  }

}
