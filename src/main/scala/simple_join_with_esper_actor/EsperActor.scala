package simple_join_with_esper_actor

import akka.actor.Actor
import com.espertech.esper.client.{Configuration, EPServiceProviderManager, UpdateListener}

class EsperActor(
    eventTypes: Map[String, Class[_]],
    eplString: String,
    updateListener: UpdateListener
) extends Actor {

  val configuration = new Configuration

  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  // Register event types with the Esper engine
  eventTypes.foreach { case (name, clazz) => configuration.addEventType(name, clazz)}

  // Create EPL statement from EPL string
  val eplStatement = administrator.createEPL(eplString)

  // Register `updateListener` with the EPL statement
  eplStatement.addListener(updateListener)

  // Receive Akka messages and send them to the Esper engine as events
  def receive = {
    case event => runtime.sendEvent(event)
  }

}
