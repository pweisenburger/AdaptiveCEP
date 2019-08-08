package adaptivecep.privacy.sgx

import java.rmi.registry.{LocateRegistry, Registry}

import adaptivecep.data.Events.Event

case class EventProcessorClient(address: String, port: Int) {

  private var remoteObject: EventProcessorServiceImpl = null

  def lookupObject(): Unit = {
    if (remoteObject == null) {
      println("\n get the registry \n")
      val registry = LocateRegistry.getRegistry(address, port)
      println(registry.toString)
      println("\n Looking up remote object \n")
      remoteObject = registry.lookup("eventProcessor").asInstanceOf[EventProcessorServiceImpl]
      println("\n done \n")
      println(remoteObject.toString)

    }
  }

  def processEvent(cond: Event => Boolean, input: Event): Boolean = {
    println("\n Calling remote object \n")
    remoteObject.applyPredicate(cond, input)

  }


}
