package simple_join_with_esper

import com.espertech.esper.client._
import stream_representation.StreamRepresentation.{Stream2, Stream4}

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

  // TODO Maybe define that upper bound directly with `Stream 2`..?
  def stream2ToArray[A <: AnyRef, B <: AnyRef](s: Stream2[A, B]): Array[AnyRef] =
    Array[AnyRef](s.t._1, s.t._2)

  // Send events to the Esper engine as events
  runtime.sendEvent(stream2ToArray(Stream2[Integer, String](42, "42")), "StreamX")
  runtime.sendEvent(stream2ToArray(Stream2[String, Integer]("13", 13)), "StreamY")

}
