package adaptivecep.privacy

import adaptivecep.data.Events.{DecryptIntAndPrintRequest, EncryptIntRequest}
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import crypto._
import crypto.cipher._

case class CryptoServiceWrapper(cryptoActor: ActorRef) extends Serializable {
  implicit val timeout = new Timeout(5 seconds)

  def encryptInt(scheme: Scheme, in: Int): EncInt = {
    val future = cryptoActor ? EncryptIntRequest(Comparable, in)
    val result = Await.result(future, timeout.duration).asInstanceOf[EncInt]
    result
  }

  def decryptAndPrint(in: EncInt): Unit = {
    cryptoActor ! DecryptIntAndPrintRequest(in)
  }
}
