package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.encryption.Encryption
import java.nio.ByteBuffer

import crypto.EncInt
import crypto.cipher.Comparable

import scala.util.Random

object ConversionRules {


  def encryptBigInt(value: Any, crypto: Encryption): Any = {
    value match {
      case e: BigInt =>
        crypto.encrypt(e.toByteArray)
      case _ => sys.error("unexpected conversion expecting BigInt to Array[Byte]")
    }
  }
  def decryptBigInt(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        BigInt(result)
      case _ => sys.error("unexpected conversion expecting Array[Byte] to BigInt")
    }
  }

  def encryptChar(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Char =>
        val buffer = ByteBuffer.allocate(2)
        crypto.encrypt(buffer.putChar(e).array())
      case _ => sys.error("unexpected input type")
    }
  }
  def decryptChar(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        ByteBuffer.wrap(result).getChar
      case _ => sys.error("unexpected type")
    }
  }

  def encryptShort(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Short =>
        val buffer = ByteBuffer.allocate(2)
        crypto.encrypt(buffer.putShort(e).array())
      case _ => sys.error("unexpected input type")
    }
  }
  def decryptShort(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        ByteBuffer.wrap(result).getShort
      case _ => sys.error("unexpected type")
    }
  }

  def encryptInt(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Int =>
        val buffer = ByteBuffer.allocate(4)
        crypto.encrypt(buffer.putInt(e).array())
      case _ => sys.error("unexpected input type")
    }
  }
  def decryptInt(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        ByteBuffer.wrap(result).getInt
      case _ => sys.error("unexpected type")
    }
  }

  def encryptString(value: Any, crypto: Encryption): Any = {
    value match {
      case str: String =>
        crypto.encrypt(str.getBytes)
      case _ => sys.error("unexpected type")
    }
  }
  def decryptString(value: Any, crypto: Encryption): Any = {
    value match {
      case arr: Array[Byte] =>
        val result = crypto.decrypt(arr)
        result.mkString
      case _ => sys.error("unexpected type")
    }
  }

  def encryptFloat(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Float =>
        val buffer = ByteBuffer.allocate(4)
        crypto.encrypt(buffer.putFloat(e).array())
      case _ => sys.error("unexpected input type")
    }
  }
  def decryptFloat(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        ByteBuffer.wrap(result).getFloat
      case _ => sys.error("unexpected type")
    }
  }

  def encryptDouble(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Double =>
        val buffer = ByteBuffer.allocate(8)
        crypto.encrypt(buffer.putDouble(e).array())
      case _ => sys.error("unexpected input type")
    }
  }
  def decryptDouble(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        ByteBuffer.wrap(result).getDouble
      case _ => sys.error("unexpected type")
    }
  }

  /***
    * in order to obfuscate the boolean single byte value
    * we generate a random positive integer and encrypt it to represent True
    * and generate random negative integer to represent False
    * @param value
    * @param crypto
    * @return
    */
  def encryptBoolean(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Boolean =>
        val randomInt = Random.nextInt(Integer.MAX_VALUE)
        val buffer = ByteBuffer.allocate(4)
        if(e){
          val posInt = buffer.putInt(randomInt).array()
          crypto.encrypt(posInt)
        }else{
          val negInt = buffer.putInt(-1 * randomInt ).array()
          crypto.encrypt(negInt)
        }
      case _ => sys.error("unexpected input type")
    }
  }
  def decryptBoolean(value: Any, crypto: Encryption): Any = {
    value match {
      case e: Array[Byte] =>
        val result = crypto.decrypt(e)
        val intVal = ByteBuffer.wrap(result).getInt
        if (intVal > 0) true else false
      case _ => sys.error("unexpected type")
    }
  }


  object IntEventTransformer extends EncDecTransformer(encryptInt, decryptInt)

  object StringEventTransformer extends EncDecTransformer(encryptString, decryptString)

  object FloatEventTransformer extends EncDecTransformer(encryptFloat, decryptFloat)

  object DoubleEventTransformer extends EncDecTransformer(encryptFloat, decryptFloat)

  object ShortEventTransformer extends EncDecTransformer(encryptShort,decryptShort)

  object CharEventTransformer extends EncDecTransformer(encryptChar,decryptChar)

  object BigIntEventTransformer extends EncDecTransformer(encryptBigInt,decryptBigInt)

  object BooleanEventTransformer extends EncDecTransformer(encryptBoolean,decryptBoolean)

  ///TODO: find a way to better retrieve those transformers by type






    def pheMapInt(value: Any,crypto: CryptoServiceWrapper): Any ={
      value match {
        case e: Int =>
          crypto.encryptInt(Comparable,e)
        case _ =>
          sys.error("unexpected data type")
      }
    }


  object IntPheSourceMapper extends PheSourceTransformer(pheMapInt)

  /***
    * this trait represents a specific transformer from some type to another
    * can be used to encrypt/decrypt
    * or to copy the data as is
    */
  sealed trait Transformer extends Serializable

  /***
    * This transformer is used to change one data element of the event to the encrypted representation of it
    * and vise versa
    * e.g.1 Int -> Array[Byte]
    * e.g.2 Employee(id: Int, name: String, salary: Int) -> EncStudent(id: Int, name: Array[Byte], salary: Array[Byte])
    * @param encrypt defines how to create the encrypted type from the given type
    *                should take the data as Any with Encryption Scheme and returns Any
    * @param decrypt defines how to get back the original data from the encrypted data
    *                takes the encrypted data as Any, and returns the original data as Any
    */
  case class EncDecTransformer(encrypt: (Any, Encryption) => Any,
                               decrypt: (Any, Encryption) => Any) extends Transformer



  case class PheSourceTransformer(map: (Any,CryptoServiceWrapper) => Any
                           ) extends Transformer

  /***
    * Disjunction Transformer is used for the disjunction Node in AdaptiveCEP
    * the event resulting from that node will carry a data type of Either[X,Y]
    * @param leftTransformer is the original transformer for type X or Left
    * @param rightTransformer is the original transformer for type Y or Right
    */
  case class DisjunctionTransformer(leftTransformer: Transformer, rightTransformer: Transformer) extends Transformer

  /***
    * represents no transformation on the data
    * when this object is encountered the data should be copied as is
    *
    */
  object NoTransformer extends Transformer

  /** *
    * Represents the event conversion rule
    */
  sealed trait EventConversionRule extends Serializable

  /** *
    * Represents Event1 Conversion rule,
    * @param tr1 carries the transformation rules for Event1
    */
  case class Event1Rule(tr1: Transformer) extends EventConversionRule

  /** *
    * Represents Event2 Conversion rule
    * @param tr1 transformation rule for the first data type the event carries
    * @param tr2 transformation rule for the second data type the event carries
    */
  case class Event2Rule(tr1: Transformer, tr2: Transformer) extends EventConversionRule

  /***
    * Represents Event3 Conversion rule
    * @param tr1 transformation rule for the first data type the event carries
    * @param tr2 transformation rule for the second data type the event carries
    * @param tr3 transformation rule for the third data type the event carries
    */
  case class Event3Rule(tr1: Transformer, tr2: Transformer, tr3: Transformer) extends EventConversionRule

  case class Event4Rule(tr1: Transformer, tr2: Transformer, tr3: Transformer, tr4: Transformer) extends EventConversionRule

  case class Event5Rule(tr1: Transformer, tr2: Transformer, tr3: Transformer, tr4: Transformer, tr5: Transformer) extends EventConversionRule

  case class Event6Rule(tr1: Transformer, tr2: Transformer, tr3: Transformer, tr4: Transformer, tr5: Transformer, tr6: Transformer) extends EventConversionRule

  def mapSource(e:Event, rule: EventConversionRule, cryptoService: CryptoServiceWrapper) : Event ={
    (e,rule) match {
      case (Event1(v1),Event1Rule(tr1)) =>
        Event1(applyMapTransformer(v1,tr1,cryptoService))
      case (Event2(v1, v2), Event2Rule(tr1, tr2)) =>
        Event2(applyMapTransformer(v1,tr1,cryptoService),
          applyMapTransformer(v2,tr2,cryptoService))

      case (Event3(v1, v2, v3),Event3Rule(tr1, tr2, tr3)) =>
        Event3(applyMapTransformer(v1,tr1,cryptoService),
          applyMapTransformer(v2,tr2,cryptoService),
          applyMapTransformer(v3,tr3,cryptoService),
        )
      case (Event4(v1, v2,v3,v4),Event4Rule(tr1, tr2, tr3, tr4)) =>
        Event4(applyMapTransformer(v1,tr1,cryptoService),
          applyMapTransformer(v2,tr2,cryptoService),
          applyMapTransformer(v3,tr3,cryptoService),
          applyMapTransformer(v4,tr4,cryptoService)
        )
      case (Event5(v1, v2,v3,v4,v5),Event5Rule(tr1, tr2, tr3, tr4, tr5)) =>
        Event5(applyMapTransformer(v1,tr1,cryptoService),
          applyMapTransformer(v2,tr2,cryptoService),
          applyMapTransformer(v3,tr3,cryptoService),
          applyMapTransformer(v4,tr4,cryptoService),
          applyMapTransformer(v5,tr5,cryptoService)
        )
      case (Event6(v1, v2,v3,v4,v5,v6),Event6Rule(tr1, tr2, tr3, tr4, tr5, tr6)) =>
        Event6(applyMapTransformer(v1,tr1,cryptoService),
          applyMapTransformer(v2,tr2,cryptoService),
          applyMapTransformer(v3,tr3,cryptoService),
          applyMapTransformer(v4,tr4,cryptoService),
          applyMapTransformer(v5,tr5,cryptoService),
          applyMapTransformer(v6,tr6,cryptoService)
        )
    }
  }

  private def applyMapTransformer(data: Any, transformer: Transformer, crypto: CryptoServiceWrapper): Any = {
    transformer match {
      case pheSourceTransformer: PheSourceTransformer =>
        pheSourceTransformer.map(data,crypto)
      case NoTransformer =>
        data
      case _ => sys.error("unexpected transformer")
    }
  }

  /***
    * applies the transformation function on data
    * @param data        the actual data carried by the event (non-encrypted in the case of EncDecTransformer)
    * @param transformer transformation rule from the raw data to another format
    *                    when the transformer is NoTransformer just copies the data as is
    * @param encryption  Encryption scheme used, notice that this object does not carry the keys
    * @return the data in the new format after applying the transformer
    */
  private def applyTransformer(data: Any, transformer: Transformer)(implicit encryption: Encryption): Any = {
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => encrypt(data, encryption)
      case DisjunctionTransformer(leftTransformer, rightTransformer) =>
        data match {
          case e: Either[Any, Any] =>
            if (e.isLeft)
              Left(applyTransformer(e.left.get, leftTransformer))
            else
              Right(applyTransformer(e.right.get, rightTransformer))
        }
    }
  }

  /**
    * this function gets the Encrypted Event EncEvent out of the normal event of type Event provided in the input
    * @param e the input unencrypted event ideally of type Event1 to Event6
    * @param conversionRule a conversion rule that corresponds the passed Event e.g. Event1 with Event1Rule
    * @param encryption the encryption scheme which should be initialized in a secure context
    * @return the encrypted representation of the event
    *         a subtype of EncEvent
    */
  def getEncryptedEvent(e: Event, conversionRule: EventConversionRule)(implicit encryption: Encryption): EncEvent = {
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

  /**
    * this function returns the original Event out of the encrypted event of type EncEvent provided in the input
    * @param e the input encrypted event ideally of type EncEvent1 to EncEvent6
    * @param conversionRule a conversion rule that corresponds the passed EncEvent e.g. EncEvent1 with Event1Rule
    * @param encryption the encryption scheme which should be initialized in a secure context
    * @return the original unencrypted event
    *         Event1 to Event6 based on the passed EncEvent
    */
  def getDecryptedEvent(e: EncEvent)(implicit encryption: Encryption): Event = {
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

  /***
    * applies the referse transformation function on the encrypted/changed data
    * @param data        the actual data carried by the event (non-encrypted in the case of EncDecTransformer)
    * @param transformer transformation rule from the raw data to another format
    *                    when the transformer is NoTransformer just copies the data as is
    * @param encryption  Encryption scheme used, notice that this object does not carry the keys
    * @return the data in the new format after applying the transformer
    */
  private def applyReverseTransformer(data: Any, transformer: Transformer)(implicit encryption: Encryption): Any = {
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => decrypt(data, encryption)
      case DisjunctionTransformer(leftTransformer, rightTransformer) =>
        data match {
          case e: Either[Any, Any] =>
            if (e.isLeft)
              Left(applyReverseTransformer(e.left.get, leftTransformer))
            else
              Right(applyReverseTransformer(e.right.get, rightTransformer))
        }
    }
  }


}
