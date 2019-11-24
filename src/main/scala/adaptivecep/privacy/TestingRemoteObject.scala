package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.ConversionRules._
import adaptivecep.privacy.encryption.{CryptoAES, Encryption}
import adaptivecep.privacy.sgx.EventProcessorClient
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
import adaptivecep.privacy.shared.Custom._


object TestingRemoteObject {
  def main(args: Array[String]): Unit = {
    try {
      val initVector = "ABCDEFGHIJKLMNOP"
      val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
      val secret = "mysecret"
      val spec = new PBEKeySpec(secret.toCharArray, "1234".getBytes(), 65536, 128)
      val factory: SecretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
      val key: Array[Byte] = factory.generateSecret(spec).getEncoded
      val skeySpec = new SecretKeySpec(key, "AES")
      implicit val encryption: Encryption = CryptoAES(skeySpec, iv)


      def cond = (x: MeasureEvent, y: Int) => {
        x.data + y > 2500
      }

      val condE = toFunEventBoolean(cond)

      val client = EventProcessorClient("13.80.151.52", 60000)
      val remoteObject = client.lookupObject()

      val measureEventTransformer = EncDecTransformer(encryptMeasureEvent, decryptMeasureEvent)

      (1 to 5000).foreach(i => {

        val input = Event2(MeasureEvent("test", i), i)
        val encEvent = getEncryptedEvent(input, Event2Rule(measureEventTransformer, IntEventTransformer))
        if (remoteObject.applyPredicate(condE, encEvent)) {
          println("condition satisfied for " + i)
        } else {
          println("condition not satisfied " + i)
        }
      })

    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }
}
