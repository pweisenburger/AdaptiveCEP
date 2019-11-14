package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.sgx.EventProcessorClient

object TestingRemoteObject {
  def main(args: Array[String]): Unit = {
    try {


      def cond = (x: Int) => {x > 2500}

      val condE = toFunEventBoolean(cond)

      val client = EventProcessorClient("52.157.152.197", 60000)
      val remoteObject = client.lookupObject()

      (1 to 5000).foreach(i => {
        val input = Event1(i)
        if (remoteObject.applyPredicate(condE, input)) {
          println("condition satisfied for " + i)
        } else {
          println("condition not satisfied "  + i)
        }
      })

    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }
}
