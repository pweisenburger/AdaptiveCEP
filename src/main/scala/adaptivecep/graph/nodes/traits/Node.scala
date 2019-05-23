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
  val bandwidthMonitorFactory: MonitorFactory
  var controller: ActorRef = self
  var created = false
  var delay: Boolean = false
  var costs: Map[ActorRef, Cost] = Map.empty[ActorRef, Cost].withDefaultValue(Cost(Duration.Zero, 100))
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

/*
  def createChildNode(
      id: Int,
      query: Query
    ): ActorRef = query match {
    case streamQuery: StreamQuery =>
      context.actorOf(Props(
        StreamNode(
          streamQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-stream")
    case sequenceQuery: SequenceQuery =>
      context.actorOf(Props(
        SequenceNode(
          sequenceQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-sequence")
    case filterQuery: FilterQuery =>
      context.actorOf(Props(
        FilterNode(
          filterQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-filter")
    case dropElemQuery: DropElemQuery =>
      context.actorOf(Props(
        DropElemNode(
          dropElemQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-dropelem")
    case selfJoinQuery: SelfJoinQuery =>
      context.actorOf(Props(
        SelfJoinNode(
          selfJoinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-selfjoin")
    case joinQuery: JoinQuery =>
      context.actorOf(Props(
        JoinNode(
          joinQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-join")
    case conjunctionQuery: ConjunctionQuery =>
      context.actorOf(Props(
        ConjunctionNode(
          conjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-conjunction")
    case disjunctionQuery: DisjunctionQuery =>
      context.actorOf(Props(
        DisjunctionNode(
          disjunctionQuery,
          publishers,
          frequencyMonitorFactory,
          latencyMonitorFactory,
          None,
          None)),
        s"$name-$id-disjunction")

  }*/

}
