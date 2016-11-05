package backend

import akka.actor.Actor
import com.espertech.esper.client.{Configuration, EPServiceProviderManager, UpdateListener}
import middleend._

trait EsperInstance {

  val configuration = new Configuration

  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  configuration.addEventType("PrimitiveInstance", classOf[PrimitiveInstance])
  configuration.addEventType("SequenceInstance", classOf[SequenceInstance])
  configuration.addEventType("AndInstance", classOf[AndInstance])
  configuration.addEventType("OrInstance", classOf[OrInstance])

  def registerEventType(name: String, clazz: Class[_]) = {
    configuration.addEventType(name, clazz)
  }

  def registerEplStringWithUpdateListener(eplString: String, UpdateListener: UpdateListener) = {
    val eplStatement = administrator.createEPL(eplString)
    eplStatement.addListener(UpdateListener)
  }

  def processEvent(event: Any) = {
    runtime.sendEvent(event)
  }

}
