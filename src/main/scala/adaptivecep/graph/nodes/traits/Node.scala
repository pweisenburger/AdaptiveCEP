package adaptivecep.graph.nodes.traits

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events.Event
import adaptivecep.data.Queries._
import adaptivecep.graph.qos._
import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}
import akka.stream.{ActorMaterializer, OverflowStrategy, SourceRef}
import akka.stream.scaladsl.{Source, SourceQueueWithComplete, StreamRefs}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

trait Node extends Actor with RequiresMessageQueue[BoundedMessageQueueSemantics]{

  val name: String = self.path.name
  val requirements: Set[Requirement]
  val publishers: Map[String, ActorRef]
  val frequencyMonitorFactory: MonitorFactory
  val latencyMonitorFactory: MonitorFactory
  var controller: ActorRef = self
  var created = false
  var delay: Boolean = false
  var costs: Map[ActorRef, Cost] = Map.empty[ActorRef, Cost].withDefaultValue(Cost(Duration.Zero, 5000))
  var emittedEvents: Int = 0
  var processedEvents: Int = 0


  val materializer = ActorMaterializer()

  var source: (SourceQueueWithComplete[Event], Source[Event, NotUsed]) = Source.queue[Event](20000, OverflowStrategy.dropNew).preMaterialize()(materializer)
  var future: Future[SourceRef[Event]] = source._2.runWith(StreamRefs.sourceRef())(materializer)
  var sourceRef: SourceRef[Event] = Await.result(future, Duration.Inf)

  def createWindow(windowType: String, size: Int): Window ={
    windowType match {
      case "SI" => SlidingInstances(size)
      case "TI" => TumblingInstances(size)
      case "ST" => SlidingTime(size)
      case "TT" => TumblingTime(size)
    }
  }
  println(name)

}
