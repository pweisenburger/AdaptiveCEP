package adaptivecep.graph.nodes.proxies

import adaptivecep.data.Events.{Created, DependenciesRequest, Event, PrepareForUpdate, TransferState, Update}
import adaptivecep.data.Queries.StreamQuery
import adaptivecep.graph.nodes.implementation.StreamNodeImpl
import adaptivecep.graph.nodes.traits.Node
import adaptivecep.graph.qos.{ChildLatencyResponse, MonitorFactory, PathLatency}
import adaptivecep.hotreplacement.Integrator
import adaptivecep.publishers.Publisher.{AcknowledgeSubscription, Subscribe}
import akka.actor.{ActorRef, Props, UnhandledMessage}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.SECONDS

case class StreamNodeProxy (
   query: StreamQuery,
   publishers: Map[String, ActorRef],
   frequencyMonitorFactory: MonitorFactory,
   latencyMonitorFactory: MonitorFactory,
   createdCallback: Option[() => Any],
   eventCallback: Option[(Event) => Any])
  extends Node{

  implicit val timeout = Timeout(5, SECONDS)
  implicit val ec: ExecutionContext = context.dispatcher

  val publisher: ActorRef = publishers(query.publisherName)

  publisher ! Subscribe

  var worker: ActorRef = null

  override def receive: Receive = {
    case DependenciesRequest =>
      (this.worker ? DependenciesRequest).pipeTo(sender())
    case AcknowledgeSubscription =>
      worker.forward(AcknowledgeSubscription)
    case ChildLatencyResponse(childNode, requestTime) => context.parent ! ChildLatencyResponse(childNode, requestTime)
    case PathLatency(childNode, duration) => context.parent ! PathLatency(childNode, duration)
    case Created => if(sender() == worker) context.parent ! Created else worker.forward(Created)
    case event: Event => if(sender() == worker) context.parent ! event else worker.forward(event)
    case Update(clazz) =>
      val future = worker ? PrepareForUpdate
      val res = Await.result(future, timeout.duration).asInstanceOf[TransferState]
      val state = res.state
      val args = (query, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      worker = context.actorOf(Props(clazz, args))
      worker ! TransferState(state)
    case unhandledMessage: UnhandledMessage =>
      worker.forward(unhandledMessage)
  }

  override def preStart(): Unit = {
    Integrator.addActor(self, this.getClass)
    worker = context.actorOf(Props(StreamNodeImpl(query, publishers, frequencyMonitorFactory, latencyMonitorFactory,
      createdCallback, eventCallback)))
    super.preStart()
  }

  override def postStop(): Unit = {
    Integrator.deleteActor(self)
    super.postStop()
  }
}
