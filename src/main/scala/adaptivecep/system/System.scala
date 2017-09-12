package adaptivecep.system

import java.util.concurrent.atomic.AtomicInteger

import adaptivecep.data.Events.{DependenciesRequest, DependenciesResponse, Event, GraphCreated}
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos._
import adaptivecep.simulation.SimulationSetup
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import distributedadaptivecep.simulation.SimulationQueries

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object System {
  val index = new AtomicInteger(0)
}

class System() {
  val actorSystem: ActorSystem = ActorSystem()
  private val roots = ListBuffer.empty[ActorRef]
  def getRoots():ListBuffer[ActorRef]  = return roots
  private val placements = mutable.Map.empty[ActorRef, Host] withDefaultValue NoHost

  private val frequencyMonitorFactory = AveragedFrequencyMonitorFactory(interval = 15, logging = true)
  private val latencyMonitorFactory = PathLatencyMonitorFactory(interval = 10, logging = true)

  def runQueryInBuilt() = {
    val publishers = SimulationSetup.publishers
    val query = SimulationQueries.queryDepth7
    val callback = None
    roots += actorSystem.actorOf(Props(query match {
      case streamQuery: StreamQuery =>
        StreamNode(streamQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case filterQuery: FilterQuery =>
        FilterNode(filterQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case selectQuery: SelectQuery =>
        SelectNode(selectQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case selfJoinQuery: SelfJoinQuery =>
        SelfJoinNode(selfJoinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case joinQuery: JoinQuery =>
        JoinNode(joinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
    }), s"root-${System.index.getAndIncrement()}")
  }

  def runQuery(query: Query, publishers: Map[String, ActorRef], callback: Option[Either[GraphCreated.type, Event] => Any]) =
    roots += actorSystem.actorOf(Props(query match {
      case streamQuery: StreamQuery =>
        StreamNode(streamQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case filterQuery: FilterQuery =>
        FilterNode(filterQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case selectQuery: SelectQuery =>
        SelectNode(selectQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case selfJoinQuery: SelfJoinQuery =>
        SelfJoinNode(selfJoinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case joinQuery: JoinQuery =>
        JoinNode(joinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
    }), s"root-${System.index.getAndIncrement()}")

  def consumers: Seq[Operator] = {
    import actorSystem.dispatcher
    implicit val timeout = Timeout(20.seconds)

    def operator(actorRef: ActorRef): Future[Operator] =
    //? is ask. i.e. ask each actor for dependency request. Actor will respond will reference of child node/s. Perform the same operation
    // on the child nodes recursively.
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

  case class ActorOperator(host: Host, dependencies: Seq[Operator], actorRef: ActorRef) extends Operator
}
