package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.sgx.EventProcessorClient

object TestingRemoteObject {
  def main(args: Array[String]): Unit = {
    try {


      def cond = (x: Int) => x > 5

      val condE = toFunEventBoolean(cond)

      val input = Event1(5)

      val client = EventProcessorClient("52.157.152.197", 60000)

      val remoteObject = client.lookupObject()
      if (remoteObject.applyPredicate(condE, input)) {
        println("condition satisfied")
      } else {
        println("condition not satisfied")

      }

    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }
}
