package adaptivecep.privacy.phe

import adaptivecep.data.Events._
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import crypto._
import crypto.cipher._
import crypto.remote.CryptoServicePlus

import scala.concurrent.Future
import scala.concurrent.duration._

/***
  * this class only
  * @param cryptoActor
  */
class CryptoServiceWrapper(cryptoActor: ActorRef) extends CryptoServicePlus with Serializable {


//  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = new Timeout(5 seconds)
//
//  def encryptInt(scheme: Scheme, in: Int): EncInt = {
//    val future = cryptoActor ? EncryptIntRequest(Comparable, in)
//    val result = Await.result(future, timeout.duration).asInstanceOf[EncInt]
//    result
//  }
//
//  def decryptAndPrint(in: EncInt): Unit = {
//    cryptoActor ! DecryptIntAndPrintRequest(in)
//  }


  override def subtract(lhs: EncInt, rhs: EncInt): Future[EncInt] = {
    val result = cryptoActor ? SubstractRequest(lhs,rhs)
    result.asInstanceOf[Future[EncInt]]
  }

  override def integerDivide(lhs: EncInt, rhs: EncInt): Future[EncInt] = {
    val result = cryptoActor ? IntegerDevideRequest(lhs,rhs)
    result.asInstanceOf[Future[EncInt]]

  }

  override def isEven(enc: EncInt): Future[Boolean] =  {
    val result = cryptoActor ? IsEvenRequest(enc)
    result.asInstanceOf[Future[Boolean]]
  }

  override def isOdd(enc: EncInt): Future[Boolean] = {
    val result = cryptoActor ? IsOddRequest(enc)
    result.asInstanceOf[Future[Boolean]]
  }

  override def splitStr(enc: EncString, regex: String): Future[List[EncString]] = {
    val result = cryptoActor ? SplitStrRequest(enc,regex)
    result.asInstanceOf[ Future[List[EncString]]]
  }

  override def floorRatio(ratio: EncRatio): Future[EncInt] = {
    val result = cryptoActor ? FloorRatioRequest(ratio)
    result.asInstanceOf[ Future[EncInt]]

  }

  override def ceilRatio(ratio: EncRatio): Future[EncInt] = {
    val result = cryptoActor ? CeilRatioRequest(ratio)
    result.asInstanceOf[ Future[EncInt]]
  }
  /** Replies with the public keys of the KeyRing used by this service */
  override def publicKeys: Future[PubKeys] = {
    val result = cryptoActor ? PublicKeysRequest
    result.asInstanceOf[Future[PubKeys]]
  }

  override def toPaillier(in: EncInt): Future[PaillierEnc] = {
    val result = cryptoActor ? ToPaillierRequest(in)
    result.asInstanceOf[Future[PaillierEnc]]
  }


  override def toElGamal(in: EncInt): Future[ElGamalEnc] = {
    val result = cryptoActor ? ToElGamalRequest(in)
    result.asInstanceOf[Future[ElGamalEnc]]
  }

  override def toAes(in: EncInt): Future[AesEnc] = {
    val result = cryptoActor ? ToAesRequest(in)
    result.asInstanceOf[Future[AesEnc]]
  }

  override def toOpe(in: EncInt): Future[OpeEnc] = {
    val result = cryptoActor ? ToOpeRequest(in)
    result.asInstanceOf[Future[OpeEnc]]
  }

  override def toAesStr(in: EncString): Future[AesString] = {
    val result = cryptoActor ? ToAesStrRequest(in)
    result.asInstanceOf[Future[AesString]]
  }

  override def toOpeStr(in: EncString): Future[OpeString] = {
    val result = cryptoActor ? ToOpeStrRequest(in)
    result.asInstanceOf[Future[OpeString]]

  }

  /** Convert the encoded value for the given scheme */
  override def convert(s: Scheme)(in: EncInt): Future[EncInt] =
    {
      val result = cryptoActor ? ConvertIntRequest(s,in)
      result.asInstanceOf[Future[EncInt]]
    }

  /** Process the list of (scheme,encoding) and convert the encoding to
    * scheme (if necessary) before replying with the whole list of
    * results
    */
  override def batchConvert(xs: List[(Scheme, EncInt)]): Future[List[EncInt]] = {
    val result = cryptoActor ? BatchConvertRequest(xs)
    result.asInstanceOf[Future[List[EncInt]]]
  }

  /** Encrypt the plain number with the given scheme NOTE: the value is
    * NOT encrypted for sending and therefore may be visible to
    * others!  If possible you should use the public keys and encrypt
    * them with an asymmetric scheme like paillier or elgamal before
    * sending it.
    */
  override def encrypt(s: Scheme)(in: Int): Future[EncInt] = {
    val future = cryptoActor ? EncryptIntRequest(Comparable, in)
    future.asInstanceOf[Future[EncInt]]
  }

  /** Process the list of (scheme,integer) and encrypt them, reply after
    * the whole list is processed
    */
  override def batchEncrypt(xs: List[(Scheme, Int)]): Future[List[EncInt]] = {
    val result = cryptoActor ? BatchEncryptRequest(xs)
    result.asInstanceOf[Future[List[EncInt]]]
  }

  /** Decrypt the value and print it locally (where the service runs) to stdout */
  override def decryptAndPrint(v: EncInt): Unit = cryptoActor ! DecryptIntAndPrintRequest(v)

  /** Print the string on the CrytpoService side */
  override def println[A](a: A): Unit = {

  }



}
