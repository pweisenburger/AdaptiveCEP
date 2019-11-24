package adaptivecep.privacy.sgx

import java.rmi.registry.{LocateRegistry, Registry}
import adaptivecep.privacy.shared.Custom._

import adaptivecep.data.Events.Event

case class EventProcessorClient(address: String, port: Int) {

//  private var remoteObject: EventProcessorServer = null

  def lookupObject(): EventProcessorServer = {
      println("\n get the registry \n")
      val registry = LocateRegistry.getRegistry(address, port)
      println(registry.toString)
      println("\n Looking up remote object \n")
      val remoteObject = registry.lookup("eventProcessor").asInstanceOf[EventProcessorServer]
      remoteObject
  }

//  def processEvent(cond: Event => Boolean, input: Event): Boolean = {
//    println("\n Calling remote object \n")
//    remoteObject.applyPredicate(cond, input)
//
//  }


}
