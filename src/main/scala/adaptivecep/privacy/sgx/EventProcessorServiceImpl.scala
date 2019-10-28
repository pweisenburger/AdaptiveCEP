package adaptivecep.privacy.sgx

import java.rmi.RemoteException

import adaptivecep.privacy.ConversionRules._
import adaptivecep.data.Events._
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.encryption._
import adaptivecep.privacy.shared.Custom._

class EventProcessorServiceImpl extends EventProcessorServer {
  /**
    * the service should decrypt the event, apply the predicate and return the result
    *
    * @param cond
    * @param input the input event for the condition
    * @throws java.rmi.RemoteException
    * @return whether the event correspond to the condition closure
    */

  val initVector = "ABCDEFGHIJKLMNOP"
  val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
  val secret = "mysecret"
  val spec = new PBEKeySpec(secret.toCharArray, "1234".getBytes(), 65536, 128)
  val factory: SecretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
  val key: Array[Byte] = factory.generateSecret(spec).getEncoded
  val skeySpec = new SecretKeySpec(key, "AES")
  implicit val encryption: Encryption = CryptoAES(skeySpec, iv)


  override def applyPredicate(cond: Event => Boolean, input: Event): Boolean = this.synchronized(
//    try {
      (cond, input) match {
        case (f: (Event1 => Boolean), e: Event1) => f(e)
        case (f: (Event2 => Boolean), e: Event2) => f(e)
        case (f: (Event3 => Boolean), e: Event3) => f(e)
        case (f: (Event4 => Boolean), e: Event4) => f(e)
        case (f: (Event5 => Boolean), e: Event5) => f(e)
        case (f: (Event6 => Boolean), e: Event6) => f(e)
        case (f: (Event1 => Boolean), e: EncEvent1) =>
          val decryptedEvent = getDecryptedEvent(e).asInstanceOf[Event1]
          f(decryptedEvent)
        case (f: (Event2 => Boolean), e: EncEvent2) =>
          val decryptedEvent = getDecryptedEvent(e).asInstanceOf[Event2]
          f(decryptedEvent)
        case (f: (Event3 => Boolean), e: EncEvent3) =>
          val decryptedEvent = getDecryptedEvent(e).asInstanceOf[Event3]
          f(decryptedEvent)
        case (f: (Event4 => Boolean), e: EncEvent4) =>
          val decryptedEvent = getDecryptedEvent(e).asInstanceOf[Event4]
          f(decryptedEvent)
        case (f: (Event5 => Boolean), e: EncEvent5) =>
          val decryptedEvent = getDecryptedEvent(e).asInstanceOf[Event5]
          f(decryptedEvent)
        case (f: (Event6 => Boolean), e: EncEvent6) =>
          val decryptedEvent = getDecryptedEvent(e).asInstanceOf[Event6]
          f(decryptedEvent)
        case _ => sys.error("unexpected type!")
      }
//    } catch {
//      case e: RemoteException => throw e
//      case e: Exception => sys.error(e.getMessage)
//    }
  ) ///sycn


}
