package adaptivecep.privacy

import adaptivecep.data.Events.{DecryptIntAndPrintRequest, EncryptIntRequest, InterpretRequest}
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
    case InterpretRequest(p) =>
      val result = remoteInterpreter.interpret(p)
      val interpreted = Await.result(result,4 seconds)
      sender() ! interpreted

    case EncryptIntRequest(s,in) =>
      val result = cryptoService.encrypt(s)(in)
      val interpreted = Await.result(result,4 seconds)
      sender() !  interpreted

    case DecryptIntAndPrintRequest(v) =>
      cryptoService.decryptAndPrint(v)
    case _ => println("recieved unknown messages")
  }

}
