package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.sgx.EventProcessorClient

object TestingRemoteObject {
  def main(args: Array[String]): Unit = {
    try {


      def cond = (x: Int) => x >5

      val condE = toFunEventBoolean(cond)

      val input = Event1(5)

      val client = EventProcessorClient("13.80.151.52", 60000)
      client.lookupObject()
      client.processEvent( condE,input )
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }
}
