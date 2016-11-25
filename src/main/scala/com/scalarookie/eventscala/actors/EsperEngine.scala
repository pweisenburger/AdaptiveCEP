package com.scalarookie.eventscala.actors

import com.espertech.esper.client._

trait EsperEngine {

  val esperServiceProviderUri: String

  val configuration = new Configuration
  // Using `lazy val`s here is inspired by https://www.lightbend.com/activator/template/akka-with-esper
  lazy val serviceProvider = EPServiceProviderManager.getProvider(esperServiceProviderUri, configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  def addEventType(eventTypeName: String, elementNames: Array[String], elementClasses: Array[Class[_]]): Unit =
    configuration.addEventType(eventTypeName, elementNames, elementClasses.asInstanceOf[Array[AnyRef]])

  def createEplStatement(eplString: String): EPStatement =
    administrator.createEPL(eplString)

  def sendEvent(eventTypeName: String, eventAsArray: Array[AnyRef]) =
    runtime.sendEvent(eventAsArray, eventTypeName)

}
