package adaptivecep.graph.nodes.traits

import com.espertech.esper.client._
import adaptivecep.data.Queries._

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
    case _: Query1[_] => Array("e1")
    case _: Query2[_, _] => Array("e1", "e2")
    case _: Query3[_, _, _] => Array("e1", "e2", "e3")
    case _: Query4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: Query5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
    case _: Query6[_, _, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5", "e6")
  }

  def createArrayOfClasses(query: Query): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    query match {
      case _: Query1[_] => Array(clazz)
      case _: Query2[_, _] => Array(clazz, clazz)
      case _: Query3[_, _, _] => Array(clazz, clazz, clazz)
      case _: Query4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: Query5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
      case _: Query6[_, _, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz, clazz)
    }
  }

  def createArrayOfNames(length: Int): Array[String] = length match {
    case 1 => Array("e1")
    case 2 => Array("e1", "e2")
    case 3 => Array("e1", "e2", "e3")
    case 4 => Array("e1", "e2", "e3", "e4")
    case 5 => Array("e1", "e2", "e3", "e4", "e5")
    case 6 => Array("e1", "e2", "e3", "e4", "e5", "e6")
  }

  def createArrayOfClasses(length: Int): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    length match {
      case 1 => Array(clazz)
      case 2 => Array(clazz, clazz)
      case 3 => Array(clazz, clazz, clazz)
      case 4 => Array(clazz, clazz, clazz, clazz)
      case 5 => Array(clazz, clazz, clazz, clazz, clazz)
      case 6 => Array(clazz, clazz, clazz, clazz, clazz, clazz)
    }
  }

  def toAnyRef(any: Any): AnyRef = {
    // Yep, an `AnyVal` can safely be cast to `AnyRef`:
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

}
