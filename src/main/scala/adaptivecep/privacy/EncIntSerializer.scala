package adaptivecep.privacy

import akka.serialization._

import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._


class EncIntSerializer extends SerializerWithStringManifest {
  override def identifier: Int = 1034555

  override def toBinary(o: AnyRef): Array[Byte] = {
    case encInt: EncInt => encInt match {
      case x: PaillierEnc => super.toBinary(x)
      case x: ElGamalEnc => super.toBinary(x)
      case x: OpeEnc => super.toBinary(x)
      case x: AesEnc => super.toBinary(x)
    }
  }

  override def manifest(o: AnyRef): String = {
    case encInt: EncInt => encInt match {
      case x: PaillierEnc => "PAILLIER"
      case x: ElGamalEnc => "ELGAMAL"
      case x: OpeEnc => "OPEENC"
      case x: AesEnc => "AESENC"
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case "PAILLIER" => super.fromBinary(bytes,PaillierEnc.getClass).asInstanceOf[PaillierEnc]
    case "ELGAMAL" => super.fromBinary(bytes,ElGamalEnc.getClass).asInstanceOf[ElGamalEnc]
    case "OPEENC" => super.fromBinary(bytes,OpeEnc.getClass).asInstanceOf[OpeEnc]
    case "AESENC" => super.fromBinary(bytes,AesEnc.getClass).asInstanceOf[AesEnc]

  }

}
