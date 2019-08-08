package adaptivecep.privacy.sgx

import java.rmi.registry.{LocateRegistry, Registry}

import adaptivecep.data.Events.Event

case class EventProcessorClient(address: String, port: Int) {
  def processEvent( cond: Event => Boolean, input: Event ) = {
    val registry = LocateRegistry.getRegistry(address, port)
    val remoteObjectPhe = registry.lookup("eventProcessor").asInstanceOf[EventProcessorServiceImpl]
    remoteObjectPhe.applyPredicate(cond,input)
  }




}
