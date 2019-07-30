package adaptivecep.privacy

import java.io.File

import akka.pattern.ask

import scala.concurrent.duration._
import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._
import argonaut._
import Argonaut._
import adaptivecep.data.Events.{DecryptIntAndPrintRequest, EncryptIntRequest, Event1}
import adaptivecep.distributed.centralized.AppRunnerCentralized.{actorSystem, address1}
import adaptivecep.distributed.centralized.EncryptionContext.keyRing
import adaptivecep.distributed.centralized.HostActorCentralized
import adaptivecep.distributed.operator.Operator
import adaptivecep.publishers.RandomPublisher
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Await, ExecutionContext}
//import adaptivecep.publishers.RandomPublisher
//import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
//import akka.remote.RemoteScope
import akka.serialization._
//
//case class PrivacyContext(keyRing: KeyRing, interpreter: LocalInterpreter)
//
//class EncIntWrapper(val json: String, val encType: String) extends Serializable {
//
//  def +(that: EncIntWrapper)(implicit pc: PrivacyContext): EncIntWrapper = {
//    //decode this and that
//    val self = EncIntWrapper.unapply(this).get
//    val other = EncIntWrapper.unapply(that).get
//    val result = pc.interpreter(self + other)
//    EncIntWrapper(result)
//  }
//
//  def _isEven() (implicit pc: PrivacyContext): Boolean ={
//    val self = EncIntWrapper.unapply(this).get
//    import crypto.dsl.isEven
//    pc.interpreter( isEven( self ))
//  }
//
//}
//
//object EncIntWrapper {
//  def apply(encInt: EncInt): EncIntWrapper = encInt match {
//    case p: PaillierEnc => new EncIntWrapper(PaillierEnc.encode(p).toString(), "PAILLIER")
//    case e: ElGamalEnc => new EncIntWrapper(ElGamalEnc.encode(e).toString(), "ELGAMAL")
//    case a: AesEnc => new EncIntWrapper(AesEnc.aesCodec.Encoder(a).toString(), "AES")
//    case o: OpeEnc => new EncIntWrapper(OpeEnc.opeCodec.Encoder(o).toString(), "OPE")
//  }
//
//  def unapply(arg: EncIntWrapper)(implicit privacyContext: PrivacyContext): Option[EncInt] =
//    arg.encType match {
//      case "PAILLIER" => PaillierEnc.decode(privacyContext.keyRing.pub.paillier).decodeJson(arg.json.parseOption.get).toOption
//      case "ELGAMAL" => ElGamalEnc.decode(privacyContext.keyRing.pub.elgamal).decodeJson(arg.json.parseOption.get).toOption
//      case "AES" => AesEnc.aesCodec.decodeJson( arg.json.parseOption.get ).toOption
//      case "OPE" => OpeEnc.opeCodec.decodeJson(arg.json.parseOption.get).toOption
//    }
//
//}
//
////
////sealed trait Student extends Serializable
////
////case class Student1[A](id: A, name: String) extends Student {
////}


object TestingEncryption extends App {


  override def main(args: Array[String]): Unit = {

//    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = new Timeout(5 seconds)

    val file = new File("application.conf")
    val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()
    val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

    val address1 = Address("akka.tcp", "ClusterSystem", "40.115.4.25", 8000)
    val host1: ActorRef = actorSystem.actorOf(Props[HostActorCentralized].withDeploy(Deploy(scope = RemoteScope(address1))), "Host" + "1")

    val cryptoActor: ActorRef = actorSystem.actorOf(Props[CryptoServiceActor].withDeploy(Deploy(scope = RemoteScope(address1))), "Crypto")

    val one = cryptoActor ? EncryptIntRequest(Comparable, 1)
    val two = cryptoActor ? EncryptIntRequest(Comparable, 2)

    val oneEnc = Await.result(one, timeout.duration).asInstanceOf[EncInt]
    val twoEnc = Await.result(two, timeout.duration).asInstanceOf[EncInt]

    cryptoActor ! DecryptIntAndPrintRequest(oneEnc)
      cryptoActor ! DecryptIntAndPrintRequest(twoEnc)

  }

}
