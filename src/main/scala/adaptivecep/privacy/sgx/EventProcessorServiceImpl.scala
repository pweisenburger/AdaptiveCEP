package adaptivecep.privacy.sgx

import adaptivecep.data.Events._
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
import adaptivecep.privacy.Privacy._
import adaptivecep.privacy.encryption.CryptoAES

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
  val encryption = CryptoAES(skeySpec, iv)


  override def applyPredicate(cond: Event => Boolean, input: Event): Boolean =
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



  private def applyTransformer(data: Any, transformer: Transformer): Any = {
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => encrypt(data, encryption)
    }
  }

  private def getEncryptedEvent(e: Event, conversionRule: EventConversionRule): EncEvent = {
    (e, conversionRule) match {
      case (Event1(e1), er: Event1Rule) => EncEvent1(applyTransformer(e1, er.tr1), er)
      case (Event2(e1, e2), er2: Event2Rule) =>
        EncEvent2(
          applyTransformer(e1, er2.tr1),
          applyTransformer(e2, er2.tr2),
          er2)
      case (Event3(e1, e2, e3), er3: Event3Rule) =>
        EncEvent3(
          applyTransformer(e1, er3.tr1),
          applyTransformer(e2, er3.tr2),
          applyTransformer(e3, er3.tr3),
          er3
        )
      case (Event4(e1, e2, e3, e4), er4: Event4Rule) =>
        EncEvent4(
          applyTransformer(e1, er4.tr1),
          applyTransformer(e2, er4.tr2),
          applyTransformer(e3, er4.tr3),
          applyTransformer(e4, er4.tr4),
          er4
        )
      case (Event5(e1, e2, e3, e4, e5), er5: Event5Rule) =>
        EncEvent5(
          applyTransformer(e1, er5.tr1),
          applyTransformer(e2, er5.tr2),
          applyTransformer(e3, er5.tr3),
          applyTransformer(e4, er5.tr4),
          applyTransformer(e5, er5.tr5),
          er5
        )
      case (Event6(e1, e2, e3, e4, e5, e6), er6: Event6Rule) =>
        EncEvent6(
          applyTransformer(e1, er6.tr1),
          applyTransformer(e2, er6.tr2),
          applyTransformer(e3, er6.tr3),
          applyTransformer(e4, er6.tr4),
          applyTransformer(e5, er6.tr5),
          applyTransformer(e6, er6.tr6),
          er6
        )
    }
  }


  private def getDecryptedEvent(e: EncEvent): Event = {
    e match {
      case EncEvent1(e1, rule) =>
        Event1(applyReverseTransformer(e1, rule.tr1))
      case EncEvent2(e1, e2, rule) =>
        Event2(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2)
        )
      case EncEvent3(e1, e2, e3, rule) =>
        Event3(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3)
        )

      case EncEvent4(e1, e2, e3, e4, rule) =>
        Event4(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3),
          applyReverseTransformer(e4, rule.tr4)
        )

      case EncEvent5(e1, e2, e3, e4, e5, rule) =>
        Event5(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3),
          applyReverseTransformer(e4, rule.tr4),
          applyReverseTransformer(e5, rule.tr5)
        )

      case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
        Event6(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3),
          applyReverseTransformer(e4, rule.tr4),
          applyReverseTransformer(e5, rule.tr5),
          applyReverseTransformer(e6, rule.tr6)
        )

    }


  }


  private def applyReverseTransformer(data: Any, transformer: Transformer): Any = {
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => decrypt(data, encryption)
    }
  }
}
