package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, PoisonPill}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source, StreamRefs}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class FilterNode(
    //query: FilterQuery,
    requirements: Set[Requirement],
    cond: Event => Boolean,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    bandwidthMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends UnaryNode {

  var parentReceived: Boolean = false
  var childCreated: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      childCreated = true
      //if (parentReceived && !created) emitCreated()
    case CentralizedCreated =>
      if(!created){
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      //println("Parent received", p1)
      parentNode = p1
      parentReceived = true
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
      //if (childCreated && !created) emitCreated()
    }
    case SourceRequest =>
      source = Source.queue[Event](20000, OverflowStrategy.dropNew).preMaterialize()(materializer)
      future = source._2.runWith(StreamRefs.sourceRef())(materializer)
      sourceRef = Await.result(future, Duration.Inf)
      sender() ! SourceResponse(sourceRef)
    case SourceResponse(ref) =>
      val s = sender()
      //println("FILTER", s)
      ref.getSource.to(Sink foreach(e =>{
        processEvent(e, s)
        //println(e)
      })).run(materializer)
    case Child1(c) => {
      //println("Child received", c)
      childNode = c
      c ! SourceRequest
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
      emitCreated()
    }
    case ChildUpdate(_, a) => {
      emitCreated()
      childNode = a
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case KillMe => sender() ! PoisonPill
    case Kill =>
      scheduledTask.cancel()
      lmonitor.scheduledTask.cancel()
      //fMonitor.scheduledTask.cancel()
      //bmonitor.scheduledTask.cancel()
      //self ! PoisonPill
      //println("Shutting down....")
    case Controller(c) =>
      controller = c
      //println("Got Controller", c)
    case CostReport(c) =>
      costs = c
      frequencyMonitor.onMessageReceive(CostReport(c), nodeData)
      latencyMonitor.onMessageReceive(CostReport(c), nodeData)
      bandwidthMonitor.onMessageReceive(CostReport(c), nodeData)
    case _: Event =>
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      bandwidthMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  def processEvent(event: Event, sender: ActorRef): Unit = {
    if (sender == childNode) {
      if (cond(event)) emitEvent(event)
    }
  }
}
