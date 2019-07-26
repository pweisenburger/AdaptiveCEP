package adaptivecep.privacy

import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._
import argonaut._
import Argonaut._
//import adaptivecep.publishers.RandomPublisher
//import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
//import akka.remote.RemoteScope
//import akka.serialization._
//
//case class PrivacyContext(keyRing: KeyRing, interpreter: LocalInterpreter)
//
//class EncIntWrapper(val json: String, val encType: String) extends Serializable {
//
//  def +(that: EncIntWrapper)(implicit pc: PrivacyContext): EncIntWrapper = {
//    //decode this and that
//    val self = EncIntWrapper.unapply(this).get
//    val other = EncIntWrapper.unapply(that).get
//    val result = pc.interpreter(self + other)
//    EncIntWrapper(result)
//  }
//
//  def _isEven() (implicit pc: PrivacyContext): Boolean ={
//    val self = EncIntWrapper.unapply(this).get
//    import crypto.dsl.isEven
//    pc.interpreter( isEven( self ))
//  }
//
//}
//
//object EncIntWrapper {
//  def apply(encInt: EncInt): EncIntWrapper = encInt match {
//    case p: PaillierEnc => new EncIntWrapper(PaillierEnc.encode(p).toString(), "PAILLIER")
//    case e: ElGamalEnc => new EncIntWrapper(ElGamalEnc.encode(e).toString(), "ELGAMAL")
//    case a: AesEnc => new EncIntWrapper(AesEnc.aesCodec.Encoder(a).toString(), "AES")
//    case o: OpeEnc => new EncIntWrapper(OpeEnc.opeCodec.Encoder(o).toString(), "OPE")
//  }
//
//  def unapply(arg: EncIntWrapper)(implicit privacyContext: PrivacyContext): Option[EncInt] =
//    arg.encType match {
//      case "PAILLIER" => PaillierEnc.decode(privacyContext.keyRing.pub.paillier).decodeJson(arg.json.parseOption.get).toOption
//      case "ELGAMAL" => ElGamalEnc.decode(privacyContext.keyRing.pub.elgamal).decodeJson(arg.json.parseOption.get).toOption
//      case "AES" => AesEnc.aesCodec.decodeJson( arg.json.parseOption.get ).toOption
//      case "OPE" => OpeEnc.opeCodec.decodeJson(arg.json.parseOption.get).toOption
//    }
//
//}
//
////
////sealed trait Student extends Serializable
////
////case class Student1[A](id: A, name: String) extends Student {
////}


object TestingEncryption extends App {


  override def main(args: Array[String]): Unit = {
    val keyRing: KeyRing = KeyRing.create
    val interpret = new LocalInterpreter(keyRing)

    val x = Common.encrypt(Comparable, keyRing)(BigInt(1))
    val y = Common.encrypt(Comparable, keyRing)(BigInt(2))

    val res_f = interpret(isEven(x))



    val res_t = interpret(isEven(y))


    println(res_f)
    println(res_t)

//    def getStudent(id: Int):Student = Student1(id,"test")
//    implicit val pc = PrivacyContext(keyRing, interpret)
//
//
//    val xJson = EncIntWrapper(x)
//    val yJson = EncIntWrapper(y)
//
//    val jwResult = xJson + yJson
//
//    //  val value1 = JsonWrapper.unapply(xJson).get
//    //  val value2 = JsonWrapper.unapply(yJson).get
//
//    //  val result = interpret(value1 + value2)
//
//    val result = EncIntWrapper.unapply(jwResult).get
//
//    val decRes = Common.decrypt(keyRing.priv)(result)
//
//    println(decRes)
//  val system = ActorSystem("snitch")
//    def sample = getStudent(3)
//
//    val serialization = SerializationExtension(system)
//    val serializer = serialization.findSerializerFor(sample)
//    val bytes = serializer.toBinary(sample)
//    val back = serializer.fromBinary(bytes, manifest = None)
//    println(s">>>>>>> pre-serialize: ${sample}")
//    println(s">>>>>>>  deserialized: ${back}")
//    system.terminate

  }

}
