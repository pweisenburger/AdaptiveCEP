package simple_join_with_esper

import com.espertech.esper.client._
import stream_representation.StreamRepresentation.Stream4

object WorkingWithArrays extends App {

  val configuration = new Configuration

  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  // Register event types with the Esper engine
  val streamXNames: Array[String] = Array("P1", "P2")
  val streamXTypes: Array[AnyRef] = Array(classOf[Int], classOf[String])
  val streamYNames: Array[String] = Array("P1", "P2")
  val streamYTypes: Array[AnyRef] = Array(classOf[String], classOf[Int])
  configuration.addEventType("StreamX", streamXNames, streamXTypes)
  configuration.addEventType("StreamY", streamYNames, streamYTypes)

  // Create EPL statement from EPL string
  val eplStatement = administrator.createEPL(
    "select * from StreamX.win:length_batch(1) as x, StreamY.win:length_batch(1) as y")

  // Register `updateListener` with the EPL statement
  eplStatement.addListener(new UpdateListener {
    def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
      val xP1: Int    = newEvents(0).get("x").asInstanceOf[Array[AnyRef]](0).asInstanceOf[Int]
      val xP2: String = newEvents(0).get("x").asInstanceOf[Array[AnyRef]](1).asInstanceOf[String]
      val yP1: String = newEvents(0).get("y").asInstanceOf[Array[AnyRef]](0).asInstanceOf[String]
      val yP2: Int    = newEvents(0).get("y").asInstanceOf[Array[AnyRef]](1).asInstanceOf[Int]
      println(Stream4[Int, String, String, Int](xP1, xP2, yP1, yP2))
    }
  })

  // Send events to the Esper engine as events
  runtime.sendEvent(Array[AnyRef](Int.box(42), "42"), "StreamX")
  runtime.sendEvent(Array[AnyRef]("13", Int.box(13)), "StreamY")

}
