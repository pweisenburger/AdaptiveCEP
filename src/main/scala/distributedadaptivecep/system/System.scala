package distributedadaptivecep.system

/**
  * Created by pratik_k on 5/26/2017.
  */
import java.util.concurrent.atomic.AtomicInteger

import adaptivecep.data.Events.{DependenciesResponse, Event, GraphCreated, _}
import adaptivecep.data.Queries.{Query, _}
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos.{AveragedFrequencyMonitorFactory, PathLatencyMonitorFactory, _}
import distributedadaptivecep.simulation.{Simulation, SimulationQueries, SimulationSetup}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

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
  private var placementsOperator = mutable.Map.empty[Int, ActiveOperator]

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

  def setPlacementsOperator(): Unit = {
    placementsOperator = mutable.Map.empty[Int, ActiveOperator]
  }
  def consumers: Seq[Operator] = {
    import actorSystem.dispatcher
    implicit val timeout = Timeout(20.seconds)
    val placementsTentaiveOperator = mutable.Set.empty[Int]

    def operator(actorRef: ActorRef): Future[Operator] =
    //? is ask. i.e. ask each actor for dependency request. Actor will respond will reference of child node/s. Perform the same operation
    // on the child nodes recursively.
      actorRef ? DependenciesRequest flatMap {
        case DependenciesResponse(dependencies) =>
          Future sequence (dependencies map operator) map {
            ActiveOperator(placements(actorRef), None, Seq.empty, _, actorRef, Map.empty)
          }
      }

    val operatorTree = Await result (Future sequence (roots map operator), timeout.duration)
    def addTenativeOperators(activeOperator: ActiveOperator): Seq[Operator] = {
      activeOperator.host match {
        case NodeHost(id, n) => n filter {neigh => !placementsTentaiveOperator.contains(neigh.asInstanceOf[NodeHost].id)} filter {neigh => !placementsOperator.contains(neigh.asInstanceOf[NodeHost].id)} map ( neighborHost => {
          val hostId = neighborHost.asInstanceOf[NodeHost].id
            val parent = activeOperator.parent.get
            parent match {
              case ActiveOperator(h, p, t, c, a, op) => {
                placementsTentaiveOperator += hostId
                if(p.isEmpty){
                  TentativeOperator(Simulation.hosts(hostId), Seq(parent), activeOperator, Map.empty)
                } else {
                  TentativeOperator(Simulation.hosts(hostId), Seq(parent) union t, activeOperator, Map.empty)
                }
              }
            }
        })
        case NoHost => Seq.empty
      }
    }
    def addParent(children: Seq[Operator], parent: Option[ActiveOperator]): Seq[Operator] = {
      children map (operator => {
        operator match {
          case ao@ActiveOperator(h, p, t, c, a, op) => {
            val opp = ActiveOperator(h, parent, t, c ,a, op)
            val tent =
              if(!ao.dependencies.isEmpty) ActiveOperator(h, parent, addTenativeOperators(opp), c ,a, op)
              else opp
            ActiveOperator(h, parent, tent.tenativeOperators, addParent(c, Option(tent)) ,a, op)
          }
        }
      })
    }
    val operatorTreeWithParent = operatorTree map (operator => {
      operator match {
        case parent@ActiveOperator(h, p, t, c, a, op) => ActiveOperator(h, p, t, addParent(c, Option(parent)), a, op)
      }
    })
    operatorTreeWithParent
  }

  def place(operator: Operator, host: Host) = {
    val ActiveOperator(_, _, _, _, actorRef, _) = operator
    if(host.isInstanceOf[NodeHost])
      placementsOperator += host.asInstanceOf[NodeHost].id -> operator.asInstanceOf[ActiveOperator]
    placements += actorRef -> host
  }
}
