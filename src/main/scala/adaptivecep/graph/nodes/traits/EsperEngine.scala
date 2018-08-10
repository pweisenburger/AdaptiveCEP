package adaptivecep.graph.nodes.traits

import com.espertech.esper.client._
import adaptivecep.data.Queries._
import shapeless.HList
import shapeless.ops.hlist.HKernelAux


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
  // hack to only let this method be invokable for arg hlistquery
  def createArrayOfNames(query: Query): Array[String] =
    throw new IllegalArgumentException("Argument has to be of type HListQuery")

  def createArrayOfNames[T <: HList](query: HListQuery[T])/*(implicit op: HKernelAux[T])*/: Array[String] =
    (for (i <- 1 to query.length) yield "e"+i).toArray

  def createArrayOfClasses(query: Query): Array[Class[_]] =
    throw new IllegalArgumentException("Argument has to be of type HListQuery")

  def createArrayOfClasses[T <: HList](query: HListQuery[T])/*(implicit op: HKernelAux[T])*/: Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    (for (i <- 1 to query.length) yield clazz).toArray
  }

  def toAnyRef(any: Any): AnyRef = {
    // Yep, an `AnyVal` can safely be cast to `AnyRef`:
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

}
