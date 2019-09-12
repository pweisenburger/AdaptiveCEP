package adaptivecep.graph.nodes.proxies

import adaptivecep.data.Events.{Created, DependenciesRequest, Event, PrepareForUpdate, TransferState, Update}
import adaptivecep.data.Queries.FilterQuery
import adaptivecep.graph.nodes.implementation.FilterNodeImpl
import adaptivecep.graph.nodes.traits.Node
import adaptivecep.graph.qos.{ChildLatencyResponse, MonitorFactory, PathLatency}
import adaptivecep.hotreplacement.Integrator
import adaptivecep.publishers.Publisher.AcknowledgeSubscription
import akka.actor.{ActorRef, Props, UnhandledMessage}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.SECONDS

case class FilterNodeProxy(
    query: FilterQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends Node{

  implicit val timeout = Timeout(5, SECONDS)
  implicit val ec: ExecutionContext = context.dispatcher

  val childNode = createChildNode(1, query.sq)
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
      val args = (query, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback, childNode)
      worker = context.actorOf(Props(clazz, args))
      worker ! TransferState(res.state)
    case unhandledMessage: UnhandledMessage =>
      println("UnhandledMessage: " + unhandledMessage)
      worker.forward(unhandledMessage)
  }

  override def preStart(): Unit = {
    Integrator.addActor(self, this.getClass)
    worker = context.actorOf(Props(FilterNodeImpl(query, publishers, frequencyMonitorFactory, latencyMonitorFactory,
      createdCallback, eventCallback, childNode)))
    super.preStart()
  }

  override def postStop(): Unit = {
    Integrator.deleteActor(self)
    super.postStop()
  }
}
