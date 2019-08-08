package adaptivecep.privacy

import adaptivecep.privacy.sgx.EventProcessorClient

object TestingRemoteObject {
  def main(args: Array[String]): Unit = {
    try {

      val client = EventProcessorClient("13.80.151.52", 60000)
      client.lookupObject()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }
}
