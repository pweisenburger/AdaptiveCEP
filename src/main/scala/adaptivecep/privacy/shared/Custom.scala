package adaptivecep.privacy.shared

import java.nio.ByteBuffer

import adaptivecep.privacy.encryption.Encryption


/** *
  * the user adds any custom classes here to be serialized to the sgx
  * trusted event processor service
  * each class MUST extend serializable otherwise this class will not be serializable with RMI
  *
  */
object Custom {

  case class CarEvent(plateNumber: String, speed: Int) extends Serializable

  case class CarEventEnc(plateNumber: Array[Byte], speed: Array[Byte]) extends Serializable

//  case class CheckPointEvent(id: Int, plateNumber: String, hour: Int, min: Int) extends Serializable

//  case class CheckPointEventEnc(id: Int, plateNumber: Array[Byte], hour: Int, min: Int) extends Serializable

  def encryptCarEvent(e: Any, encryption: Encryption): Any = {
    e match {
      case CarEvent(plateNumber, speed) =>
        CarEventEnc(encryption.encryptString(plateNumber), encryption.encryptInt(speed))
      case _ => sys.error("unexpected event type!")
    }
  }

  def decryptCarEvent(e: Any, encryption: Encryption): Any = {
    e match {
      case CarEventEnc(plateNumber, speed) =>
        CarEvent(encryption.decryptString(plateNumber), encryption.decryptInt(speed))
      case _ => sys.error("unexpected event type!")
    }
  }

//  def encryptCheckPointEvent(e: Any, encryption: Encryption): Any = {
//    e match {
//      case CheckPointEvent(id,plateNumber,h,m) =>
//        CheckPointEventEnc(id,encryption.encryptString(plateNumber),h,m)
//      case _ => sys.error("unexpected event type!")
//    }
//  }
//
//  def decryptCheckPointEvent(e: Any, encryption: Encryption): Any = {
//    e match {
//      case CheckPointEventEnc(id,plateNumber,h,m) =>
//        CheckPointEvent(id,encryption.decryptString(plateNumber),h,m)
//      case _ => sys.error("unexpected event type")
//    }
//  }

}
