package adaptivecep.privacy.shared

import java.nio.ByteBuffer

import adaptivecep.privacy.encryption.Encryption


/***
  * the user adds any custom classes here to be serialized to the sgx
  * trusted event processor service
  * each class MUST extend serializable otherwise this class will not be serializable with RMI
  *
  */
object Custom {

  case class Employee(name: String, salary: Int) extends Serializable

  case class EncEmployee(name: String, salary: Array[Byte]) extends Serializable

  def empEncrypt(emp: Any, encryption: Encryption): Any = {
    emp match {
      case Employee(name,salary) =>
        val buffer = ByteBuffer.allocate(4)
        EncEmployee(name, encryption.encrypt(buffer.putInt(salary).array()))
      case _ => sys.error("unexpected data type")
    }
  }
  def empDecrypt(encEmp: Any, encryption: Encryption): Any = {
    encEmp match {
      case EncEmployee(name,encSalary) =>
        val result = encryption.decrypt(encSalary)
        val salary = ByteBuffer.wrap(result).getInt
        Employee(name,salary)
      case _ => sys.error("unexpected data type")
    }
  }


}
