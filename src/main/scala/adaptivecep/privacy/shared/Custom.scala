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

  case class Employee(id: Int, name: String, salary: Int) extends Serializable

  case class EncEmployee(id: Int, name: Array[Byte], salary: Array[Byte]) extends Serializable

  def empEncrypt(emp: Any, encryption: Encryption): Any = {
    emp match {
      case Employee(id,name,salary) =>
        EncEmployee(id, encryption.encryptString(name), encryption.encryptInt(salary))
      case _ => sys.error("unexpected data type")
    }
  }
  def empDecrypt(encEmp: Any, encryption: Encryption): Any = {
    encEmp match {
      case EncEmployee(id, encName,encSalary) =>
        val name = encryption.decryptString(encName)
        val salary = encryption.decryptInt(encSalary)
        Employee(id, name,salary)
      case _ => sys.error("unexpected data type")
    }
  }

  case class EntryEvent(employeeId: Int, hour: Int, minute: Int, second: Int) extends Serializable

}
