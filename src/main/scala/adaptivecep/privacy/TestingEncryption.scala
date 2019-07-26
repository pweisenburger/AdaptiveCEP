package adaptivecep.privacy

import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._
import argonaut._


case class PrivacyContext(keyRing: KeyRing, interpreter: LocalInterpreter)

class EncIntWrapper(val json: Json, val encType: String) extends Serializable {

  def +(that: EncIntWrapper)(implicit pc: PrivacyContext): EncIntWrapper = {
    //decode this and that
    val self = EncIntWrapper.unapply(this).get
    val other = EncIntWrapper.unapply(that).get
    val result = pc.interpreter(self + other)
    EncIntWrapper(result)
  }

  def _isEven() (implicit pc: PrivacyContext): Boolean ={
    val self = EncIntWrapper.unapply(this).get
    import crypto.dsl.isEven
    pc.interpreter( isEven( self ))
  }

}

object EncIntWrapper {
  def apply(encInt: EncInt): EncIntWrapper = encInt match {
    case p: PaillierEnc => new EncIntWrapper(PaillierEnc.encode(p), "PAILLIER")
    case e: ElGamalEnc => new EncIntWrapper(ElGamalEnc.encode(e), "ELGAMAL")
    case a: AesEnc => new EncIntWrapper(AesEnc.aesCodec.Encoder(a), "AES")
    case o: OpeEnc => new EncIntWrapper(OpeEnc.opeCodec.Encoder(o), "OPE")
  }

  def unapply(arg: EncIntWrapper)(implicit privacyContext: PrivacyContext): Option[EncInt] =
    arg.encType match {
      case "PAILLIER" => PaillierEnc.decode(privacyContext.keyRing.pub.paillier).decodeJson(arg.json).toOption
      case "ELGAMAL" => ElGamalEnc.decode(privacyContext.keyRing.pub.elgamal).decodeJson(arg.json).toOption
      case "AES" => AesEnc.aesCodec.decodeJson(arg.json).toOption
      case "OPE" => OpeEnc.opeCodec.decodeJson(arg.json).toOption
    }

}


object TestingEncryption extends App {

  override def main(args: Array[String]): Unit = {

    val keyRing: KeyRing = KeyRing.create
    val interpret = new LocalInterpreter(keyRing)
    implicit val pc = PrivacyContext(keyRing, interpret)

    val x = Common.encrypt(Comparable, keyRing)(BigInt(1))
    val y = Common.encrypt(Comparable, keyRing)(BigInt(2))

    val xJson = EncIntWrapper(x)
    val yJson = EncIntWrapper(y)

    val jwResult = xJson + yJson

    //  val value1 = JsonWrapper.unapply(xJson).get
    //  val value2 = JsonWrapper.unapply(yJson).get

    //  val result = interpret(value1 + value2)

    val result = EncIntWrapper.unapply(jwResult).get

    val decRes = Common.decrypt(keyRing.priv)(result)

    println(decRes)


  }

}
