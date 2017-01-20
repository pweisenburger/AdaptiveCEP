package com.scalarookie.eventscala.graph.nodes

import com.espertech.esper.client._

trait EsperEngine {

  val esperServiceProviderUri: String

  val configuration = new Configuration
  // Using `lazy val`s here is inspired by https://www.lightbend.com/activator/template/akka-with-esper
  lazy val serviceProvider: EPServiceProvider =
    EPServiceProviderManager.getProvider(esperServiceProviderUri, configuration)
  lazy val runtime: EPRuntime = serviceProvider.getEPRuntime
  lazy val administrator: EPAdministrator = serviceProvider.getEPAdministrator

  def addEventType(eventTypeName: String, elementNames: Array[String], elementClasses: Array[Class[_]]): Unit =
    configuration.addEventType(eventTypeName, elementNames, elementClasses.asInstanceOf[Array[AnyRef]])

  def createEplStatement(eplString: String): EPStatement =
    administrator.createEPL(eplString)

  def sendEventToEngine(eventTypeName: String, eventAsArray: Array[AnyRef]): Unit =
    runtime.sendEvent(eventAsArray, eventTypeName)

  def destoryServiceProvider(): Unit =
    serviceProvider.destroy()

}
