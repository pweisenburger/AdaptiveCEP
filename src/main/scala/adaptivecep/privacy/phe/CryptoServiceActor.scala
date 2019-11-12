package adaptivecep.privacy.phe

import adaptivecep.data.Events._
import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}
import crypto._
import crypto.dsl._
import crypto.remote.{CryptoServiceImpl, CryptoServicePlus}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/** *
  * this actor should be deployed on a Trusted host
  * it initializes the crypto services and the remote interpreter
  *
  */
class CryptoServiceActor extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics] {
  val cluster: Cluster = Cluster(context.system)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val keyRing: KeyRing = KeyRing.create
  val cryptoService: CryptoServicePlus = new CryptoServiceImpl(keyRing)

  //TODO: remove this interpreter as it will not be used anymore
  val remoteInterpreter = RemoteInterpreter(cryptoService, keyRing)

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

    //TODO: delete this request, it is no longer used
    case InterpretRequest(p) =>
      val result = remoteInterpreter.interpret(p)
      val interpreted = Await.result(result, 4 seconds)
      sender() ! interpreted

    case EncryptIntRequest(s, in) =>
      val result = cryptoService.encrypt(s)(in)
      val encrypted = Await.result(result, 4 seconds)
      sender() ! encrypted

    case IsEvenRequest(enc: EncInt) =>
      println("received (is even request)")
      val result = cryptoService.isEven(enc)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case IsOddRequest(enc) =>
      println("received (is odd request)")
      val result = cryptoService.isOdd(enc)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case SplitStrRequest(enc, regex) =>
      println("received (split string request)")
      val result = cryptoService.splitStr(enc, regex)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult


    case DecryptIntAndPrintRequest(v) =>
      cryptoService.decryptAndPrint(v)

    case ToOpeRequest(in: EncInt) =>
      println("received (to ope request)")
      val result = cryptoService.toOpe(in)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case ToAesRequest(in: EncInt) =>
      println("received (to aes request)")
      val result = cryptoService.toAes(in)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case ToElGamalRequest(in: EncInt) =>
      println("received (to elgamal request)")
      val result = cryptoService.toElGamal(in)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case ToPaillierRequest(in: EncInt) =>
      println("received (to paillier request) ")
      val result = cryptoService.toPaillier(in)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case PublicKeysRequest =>
      val result = cryptoService.publicKeys
      val keys = Await.result(result, 4 seconds)
      sender() ! keys
    //TODO: complete the rest of functionalities

    case ConvertIntRequest(s, in) =>
      println("received (convert int request) ")
      val result = cryptoService.convert(s)(in)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case SubstractRequest(lhs, rhs) =>
      println("received (substract request) ")
      val result = cryptoService.subtract(lhs, rhs)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case IntegerDevideRequest(lhs, rhs) =>
      println("received (int divide request) ")
      val result = cryptoService.integerDivide(lhs, rhs)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case FloorRatioRequest(ratio) =>
      println("received (floor ratio request) ")
      val result = cryptoService.floorRatio(ratio)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case CeilRatioRequest(ratio) =>
      println("received (floor ratio request) ")
      val result = cryptoService.ceilRatio(ratio)
      val actualResult = Await.result(result, 4 seconds)
      sender() ! actualResult

    case PrintLineRequest(a) =>
      println(a)

    case m => println("recieved unknown messages " + m)
  }

}
