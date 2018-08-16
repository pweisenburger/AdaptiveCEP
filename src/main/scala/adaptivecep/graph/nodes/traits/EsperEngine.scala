package adaptivecep.graph.nodes.traits

import adaptivecep.data.Queries._
import com.espertech.esper.client._


trait EsperEngine {

  val esperServiceProviderUri: String

  val configuration = new Configuration

  // Using `lazy val`s here is inspired by Frank Sauer's template `akka-with-esper`:
  // https://www.lightbend.com/activator/template/akka-with-esper
  lazy val serviceProvider: EPServiceProvider =
    EPServiceProviderManager.getProvider(esperServiceProviderUri, configuration)
  lazy val runtime: EPRuntime = serviceProvider.getEPRuntime
  lazy val administrator: EPAdministrator = serviceProvider.getEPAdministrator

  def addEventType(eventTypeName: String, elementNames: Array[String], elementClasses: Array[Class[_]]): Unit = {
    configuration.addEventType(eventTypeName, elementNames, elementClasses.asInstanceOf[Array[AnyRef]])
  }

  def createEpStatement(eplString: String): EPStatement = {
    administrator.createEPL(eplString)
  }

  def sendEvent(eventTypeName: String, eventAsArray: Array[AnyRef]): Unit = {
    runtime.sendEvent(eventAsArray, eventTypeName)
  }

  def destroyServiceProvider(): Unit = {
    serviceProvider.destroy()
  }
}

object EsperEngine {
  def createArrayOfNames(query: Query): Array[String] = query match {
    case hquery:HListQuery[_] => (for (i <- 1 to hquery.length) yield "e"+i).toArray
    case _ => throw new IllegalArgumentException("Argument has to be of type HListQuery")
  }

  def createArrayOfClasses(query: Query): Array[Class[_]] = query match {
    case hquery:HListQuery[_] =>
      val clazz: Class[_] = classOf[AnyRef]
      (for (i <- 1 to hquery.length) yield clazz).toArray
    case _ => throw new IllegalArgumentException("Argument has to be of type HListQuery")
  }

  def toAnyRef(any: Any): AnyRef = {
    // Yep, an `AnyVal` can safely be cast to `AnyRef`:
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

}
