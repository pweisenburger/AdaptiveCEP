package adaptivecep.system

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import adaptivecep.data.Events.{DependenciesRequest, DependenciesResponse, Event, Created}
import adaptivecep.data.Queries._
import adaptivecep.graph._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object System {
  val index = new AtomicInteger(0)
}

class System(implicit actorSystem: ActorSystem) {
  private val roots = ListBuffer.empty[ActorRef]
  private val placements = mutable.Map.empty[ActorRef, Host] withDefaultValue NoHost

  private val frequencyMonitorFactory = AverageFrequencyMonitorFactory(interval = 15, logging = true)
  private val latencyMonitorFactory = PathLatencyMonitorFactory(interval = 10, logging = true)

  def runQuery(query: Query, publishers: Map[String, ActorRef], createdCallback: Option[() => Any], eventCallback: Option[(Event) => Any]) =
    roots += actorSystem.actorOf(Props(query match {
      case streamQuery: StreamQuery =>
        StreamNode(streamQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      case sequenceQuery: SequenceQuery[_, _] =>
        SequenceNode(sequenceQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      case filterQuery: FilterQuery =>
        FilterNode(filterQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      case dropElemQuery: DropElemQuery =>
        DropElemNode(dropElemQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      case selfJoinQuery: SelfJoinQuery =>
        SelfJoinNode(selfJoinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      case joinQuery: JoinQuery =>
        JoinNode(joinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      case conjunctionQuery: ConjunctionQuery =>
        ConjunctionNode(conjunctionQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)
      /*case disjunctionQuery: DisjunctionQuery =>
        DisjunctionNode(disjunctionQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, createdCallback, eventCallback)*/
    }), s"root-${System.index.getAndIncrement()}")

  def consumers: Seq[Operator] = {
    import actorSystem.dispatcher
    implicit val timeout = Timeout(20.seconds)

    def operator(actorRef: ActorRef): Future[Operator] =
      actorRef ? DependenciesRequest flatMap {
        case DependenciesResponse(dependencies) =>
          Future sequence (dependencies map operator) map {
            ActorOperator(placements(actorRef), _, actorRef)
          }
      }

    Await result (Future sequence (roots map operator), timeout.duration)
  }

  def place(operator: Operator, host: Host) = {
    val ActorOperator(_, _, actorRef) = operator
    placements += actorRef -> host
  }

  private case class ActorOperator(host: Host, dependencies: Seq[Operator], actorRef: ActorRef) extends Operator
}
