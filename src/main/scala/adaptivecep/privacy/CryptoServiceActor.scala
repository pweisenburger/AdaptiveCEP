package adaptivecep.privacy

import adaptivecep.data.Events._
import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}
import crypto._
import crypto.dsl._
import crypto.cipher._

import scala.concurrent.duration._
import crypto.remote.{CryptoServiceImpl, CryptoServicePlus}

import scala.concurrent.{Await, ExecutionContext}

/***
  * this actor should be deployed on a Trusted host
  * it initializes the crypto services and the remote interpreter
  *
  */
class CryptoServiceActor extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics]{
  val cluster: Cluster = Cluster(context.system)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val keyRing: KeyRing = KeyRing.create
  val cryptoService: CryptoServicePlus = new CryptoServiceImpl(keyRing)
  val remoteInterpreter = RemoteInterpreter(cryptoService,keyRing)

  override def preStart() = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def receive: Receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)

      /////////////////////////// Crypto Requests

    case InterpretRequest(p) =>
      val result = remoteInterpreter.interpret(p)
      val interpreted = Await.result(result,4 seconds)
      sender() ! interpreted

    case EncryptIntRequest(s,in) =>
      val result = cryptoService.encrypt(s)(in)
//      val interpreted = Await.result(result,4 seconds)
      sender() !  result

    case IsEvenRequest(enc: EncInt) =>
      val result = cryptoService.isEven(enc)
      sender() ! result

    case DecryptIntAndPrintRequest(v) =>
      cryptoService.decryptAndPrint(v)

    case ToOpeRequest(in: EncInt) =>
      val result = cryptoService.toOpe(in)
      sender() ! result

    case ToAesRequest(in: EncInt) =>
      val result = cryptoService.toAes(in)
      sender() ! result

    case ToElGamalRequest(in: EncInt) =>
      val result = cryptoService.toElGamal(in)
      sender() ! result

    case ToPaillierRequest(in: EncInt) =>
      val result = cryptoService.toPaillier(in)
      sender() ! result

    case PublicKeysRequest =>
      val result = cryptoService.publicKeys
//      val keys = Await.result(result, 4 seconds)
//      sender() ! PublicKeysResponse(keys)
      sender() ! result
    //TODO: complete the rest of functionalities

    case m => println("recieved unknown messages " + m)
  }

}
