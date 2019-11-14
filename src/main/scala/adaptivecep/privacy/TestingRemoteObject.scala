package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.sgx.EventProcessorClient

object TestingRemoteObject {
  def main(args: Array[String]): Unit = {
    try {


      def cond = (x: Int) => x > 2500

      val condE = toFunEventBoolean(cond)

      val client = EventProcessorClient("13.80.151.52", 60000)
      val remoteObject = client.lookupObject()
      val input = Event1(5)
      if (remoteObject.applyPredicate(condE, input)) {
        println("condition satisfied")
      } else {
        println("condition not satisfied")
      }
//      (1 to 5000).foreach(i => {
//
//      })

    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }
}
