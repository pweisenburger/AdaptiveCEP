package simple_join_with_esper

import com.espertech.esper.client._
import stream_representation.StreamRepresentation._

import scala.reflect.ClassTag

object NotWorking extends App {

  def classForT[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass

  val configuration = new Configuration

  lazy val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  lazy val runtime = serviceProvider.getEPRuntime
  lazy val administrator = serviceProvider.getEPAdministrator

  // Register event types with the Esper engine
  configuration.addEventType("StreamX", classForT[Stream2[Int, String]])
  configuration.addEventType("StreamY", classForT[Stream2[String, Int]])

  // Create EPL statement from EPL string
  val eplStatement = administrator.createEPL(
    "select * from StreamX.win:length_batch(1) as x, StreamY.win:length_batch(1) as y")

  // Register `updateListener` with the EPL statement
  eplStatement.addListener(new UpdateListener {
    def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
      val x = newEvents(0).get("x").asInstanceOf[Stream2[Int, String]]
      val y = newEvents(0).get("y").asInstanceOf[Stream2[String, Int]]
      val res: Stream4[Int, String, String, Int] = join22(x, y)
      println(res)
    }
  })

  // Send events to the Esper engine as events
  runtime.sendEvent(Stream2[Int, String](42, "42"))
  runtime.sendEvent(Stream2[String, Int]("13", 13))

}
