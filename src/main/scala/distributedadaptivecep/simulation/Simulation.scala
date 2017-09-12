package distributedadaptivecep.simulation

import distributedadaptivecep.simulation.Simulation._
import distributedadaptivecep.system._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.Random

object Simulation {
  //contains latency and bandwidth measurement for each Host
  case class HostProps(
      latency: Seq[(Host, ContinuousBoundedValue[Duration])],
      bandwidth: Seq[(Host, ContinuousBoundedValue[Double])]) {
    def advance = HostProps(
      latency map { case (host, latency) => (host, latency.advance) },
      bandwidth map { case (host, bandwidth) => (host, bandwidth.advance) })
  }

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing

  val random = new Random(0)

  val nodeCount = 25
  val neighbors = 7
  val stepDuration = 1.second

  val hosts: Map[Int, NodeHost]  = (0 until nodeCount map (id => id -> NodeHost(id, assignNeighbors))).toMap
  private def assignNeighbors() : Seq[Host] = {
    0 until neighbors map (neighborNo => {
      val hostId = random.nextInt(( nodeCount - 1));
      NodeHost(hostId, Seq.empty)
      //hosts.getOrElse(hostId, NodeHost(hostId, Seq.empty))
    })
  }
}

class Simulation(system: System) {
  val random = new Random(0)

  val nodeCount = 25
  val neighbors = 7
  val stepDuration = 1.second


  object latency {
    implicit val addDuration: (Duration, Duration) => Duration = _ + _

    val template = ContinuousBoundedValue[Duration](
      Duration.Undefined,
      min = 2.millis, max = 100.millis,
      () => (0.4.millis - 0.8.milli * random.nextDouble, 1 + random.nextInt(900)))

    def apply() =
      template copy (value = 2.milli + 98.millis * random.nextDouble)
  }

  object bandwidth {
    implicit val addDouble: (Double, Double) => Double = _ + _

    val template = ContinuousBoundedValue[Double](
      0,
      min = 5, max = 100,
      () => (0.4 - 0.8 * random.nextDouble, 1 + random.nextInt(900)))

    def apply() =
    //lets you make a copy of an object, you can change fields as desired during the copying process.
    //creates a copy of ContinuousBoundedValue object with a new value between 5 and 95
      template copy (value = 5 + 95* random.nextDouble)
  }

  //map which contains latency and bandwidth of one host to all other hosts
  private var hostProps: Map[Host, HostProps] = {
    (hosts.values.toSeq map { id =>
      id -> HostProps(
        hosts.values.toList collect { case otherId if otherId != id => otherId -> latency() },
        hosts.values.toList collect { case otherId if otherId != id => otherId -> bandwidth() })
    }).toMap
  }
  private def latencySelector(props: HostProps, host: Host): Duration =
    (props.latency collectFirst { case (`host`, latency) => latency }).get.value

  private def bandwidthSelector(props: HostProps, host: Host): Double =
    (props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }).get.value

  private def latencyBandwidthSelector(props: HostProps, host: Host): (Duration, Double) =
    ((props.latency collectFirst { case (`host`, latency) => latency }).get.value,
     (props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }).get.value)

  private def avg(durations: Seq[Duration]): Duration =
    if (durations.isEmpty)
      Duration.Zero
    else
      durations.foldLeft[Duration](Duration.Zero) { _ + _ } / durations.size

  private def avg(numerics: Seq[Double]): Double =
    if (numerics.isEmpty)
      0.0
    else
      numerics.sum / numerics.size

  private var time = Duration.Zero


  def currentTime = time

  def advance(): Unit = {
    //If xs is some collection, then xs.view is the same collection, but with all transformers implemented lazily.
    // To get back from a view to a strict collection, you can use the force method. https://www.scala-lang.org/docu/files/collections-api/collections_42.html
    hostProps = (hostProps mapValues { _.advance }).view.force
    time += stepDuration
  }

  def measureLatency: Duration = {
    val lat = measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avg } { _.host }
    lat
  }
  def measureBandwidth: Double =
    measure(bandwidthSelector, Maximizing, Double.MaxValue) { math.min } { avg } { _.host }

  private def measure[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing,
      zero: T)(
      merge: (T, T) => T)(
      avg: Seq[T] => T)(
      host: Operator => Host): T = {
    def measure(operator: Operator): T =
      if (operator.dependencies.isEmpty)
        zero
      else {
    //finds the total latency/bandwidth from the root node till the child node
        val list = operator.dependencies map { dependentOperator => {
            merge(measure(dependentOperator), selector(hostProps(host(operator)), host(dependentOperator)))
          }
        }
        val value = minmax(optimizing, list)
        value
      }
    avg(system.consumers map measure)
  }

  def placeSequentially(): Unit = {
    def allOperators(operator: Operator): Seq[Operator] =
      operator +: (operator.dependencies flatMap allOperators)

    var nodeIndex = 0

    def placeOperator(operator: Operator): Unit = {
      if (nodeIndex >= nodeCount)
        throw new UnsupportedOperationException("not enough hosts")

      system place (operator, hosts(nodeIndex))
      nodeIndex += 1

      operator.dependencies foreach placeOperator
    }
    system.consumers foreach placeOperator
  }

  def placeOptimizingLatency(): Unit = {
    //A partial function is returned by measure due to the _ as the last parameter

    val (durationB, placementsB) = placeDistibutedOptimizingHeuristic(latencySelector, Minimizing) { _ + _ } { avg }

    system.setPlacementsOperator()
    (placementsB) foreach { case (host, operator) =>
      system place (operator, host)
    }
  }

  def placeOptimizingBandwidth(): Unit = {

    val (durationB, placementsB) = placeDistibutedOptimizingHeuristic(bandwidthSelector, Maximizing) { math.min } { avg }

    system.setPlacementsOperator()
    (placementsB) foreach { case (host, operator)  =>
      system place (operator, host)
    }
  }

  def placeOptimizingLatencyAndBandwidth(): Unit = {
    def average(durationNumerics: Seq[(Duration, Double)]): (Duration, Double) =
      durationNumerics.unzip match { case (latencies, bandwidths) => (avg(latencies), avg(bandwidths)) }

    def merge(durationNumeric0: (Duration, Double), durationNumeric1: (Duration, Double)): (Duration, Double) =
      (durationNumeric0, durationNumeric1) match { case ((duration0, numeric0), (duration1, numeric1)) =>
        (duration0 + duration1, math.min(numeric0, numeric1))
      }

    implicit val ordering = new Ordering[(Duration, Double)] {
      def abs(x: Duration) = if (x < Duration.Zero) -x else x
      def compare(x: (Duration, Double), y: (Duration, Double)) = ((-x._1, x._2), (-y._1, y._2)) match {
        case ((d0, n0), (d1, n1)) if d0 == d1 && n0 == n1 => 0
        case ((d0, n0), (d1, n1)) if d0 < d1 && n0 < n1 => -1
        case ((d0, n0), (d1, n1)) if d0 > d1 && n0 > n1 => 1
        case ((d0, n0), (d1, n1)) =>
          math.signum((d0 - d1) / abs(d0 + d1) + (n0 - n1) / math.abs(n0 + n1)).toInt
      }
    }

    val (durationB, placementsB) = placeDistibutedOptimizingHeuristic(latencyBandwidthSelector, Maximizing) { merge } { average }
    //val durationB = measureLatency { placementsB(_) }

    system.setPlacementsOperator()
    (placementsB) foreach { case (host, operator) =>
      system place (operator, host)
    }

  }

  private def placeDistibutedOptimizingHeuristic[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing)(merge: (T, T) => T)(
      avg: Seq[T] => T): (T, collection.Map[Host, Operator]) = {
    val placements = mutable.Map.empty[Host, Operator]

    def placeOptimizedPath(operator: Operator): Unit = {
      operator.optimumPath.values foreach { placeOptimizedPath }
      operator match {
        case ao@ActiveOperator(h, p, t, c, a, o) => placements += h -> ao
        case to@TentativeOperator(h, p, n, o)  => placements += h -> n
      }
    }

    def getOptimumPath(operator: Operator): Operator = {
      operator match {
        case ao@ActiveOperator(hp, p, t, c, a, op) => {
          val optimumChild = c  map { child =>
            child match {
              case  ActiveOperator(h, p, t, c, a, o) => {
                val operatorToChild: Map[T, Operator] =
                  if(o.nonEmpty)
                    Map(merge(selector(hostProps(hp), child.host), minmax(optimizing, o.keys.asInstanceOf[TraversableOnce[T]])) -> child)
                  else
                    Map(selector(hostProps(hp), child.host) -> child)
                val operatorToTentativeOpOfChild:Seq[(T, Operator)] = t map { top =>
                  top match {
                    case TentativeOperator(h, p, n, o) => {
                      if(o.nonEmpty)
                        merge(selector(hostProps(hp), top.host), minmax(optimizing, o.keys.asInstanceOf[TraversableOnce[T]])) -> top
                      else {
                        selector(hostProps(hp), top.host) -> top
                      }
                    }
                    case ActiveOperator(h, p, t, c, a, o) => {
                      if(o.nonEmpty)
                        merge(selector(hostProps(hp), top.host), minmax(optimizing, o.keys.asInstanceOf[TraversableOnce[T]])) -> top
                      else {
                        selector(hostProps(hp), top.host) -> top
                      }
                    }
                  }
                }
                val (min, optimumOperator) = minmaxBy(optimizing, operatorToTentativeOpOfChild ++ operatorToChild) { case (value, _) => value }
                min -> optimumOperator
              }
            }
          }
         val modifiedTentativeOp = ao.tenativeOperators map { tentativeOp =>
           tentativeOp match {
             case TentativeOperator(hpt, p, n, op) => {
               val optimumChild = c map { child =>
                 child match {
                   case  ActiveOperator(h, p, t, c, a, o) => {
                     val operatorToChild: Map[T, Operator] =
                       if(o.nonEmpty)
                         Map(merge(selector(hostProps(hpt), child.host), minmax(optimizing, o.keys.asInstanceOf[TraversableOnce[T]])) -> child)
                       else
                         Map(selector(hostProps(hpt), child.host) -> child)
                     val operatorToTentativeOpOfChild: Seq[(T, Operator)] = t  map { top =>
                       top match {
                         case TentativeOperator(h, p, a, o) => {
                           if(o.nonEmpty)
                             merge(selector(hostProps(hpt), top.host), minmax(optimizing, o.keys.asInstanceOf[TraversableOnce[T]])) -> top
                           else {
                             selector(hostProps(hpt), top.host) -> top
                           }
                         }
                         case ActiveOperator(h, p, t, c, a, o) => {
                           if(o.nonEmpty)
                             merge(selector(hostProps(hpt), top.host), minmax(optimizing, o.keys.asInstanceOf[TraversableOnce[T]])) -> top
                           else {
                             selector(hostProps(hpt), top.host) -> top
                           }
                         }
                       }
                     }
                     val (min, optimumOperator) = minmaxBy(optimizing, operatorToTentativeOpOfChild ++ operatorToChild) { case (value, _) => value }
                     min -> optimumOperator
                   }
                 }
               }
               TentativeOperator(hpt, p, n, optimumChild.toMap)
             }
             case a0@ActiveOperator(h, p, t, c, a, o) => a0
           }
         }
          ActiveOperator(hp, p, modifiedTentativeOp, c, a, optimumChild.toMap)
        }
      }
    }
    /*Calculates the cost of path from source node to consumer node via each Active/Tentative operator*/
    def findOptimizedPath(operator: Operator): Operator = {
      val children = operator.dependencies map { findOptimizedPath }
      operator match {
        case  ActiveOperator(h, p, t, c, a, o) => {
          //Bottom up approach - starts from the bottom intermediate node
          if(operator.dependencies.nonEmpty) {
            /*gets optimum path of an operator. Finds optimum children, stores the optimum path so that it's parent can access the optimum
            value of latency/bandwidth till this node
            */
            getOptimumPath(ActiveOperator(h, p, t, children, a, o))
          } else {
            operator
          }
        }
      }
    }
    val consumers = system.consumers map { findOptimizedPath }
    consumers foreach { placeOptimizedPath }
    val value = avg(consumers map {consumer => minmax(optimizing, consumer.optimumPath.keys.asInstanceOf[TraversableOnce[T]])})
    (value, placements)
  }
  private def placeOptimizingHeuristicA[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing): collection.Map[Operator, Host] = {
    val placements = mutable.Map.empty[Operator, Host]

    def placeProducersConsumers(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeProducersConsumers(_, consumer = false) }
      if (consumer || operator.dependencies.isEmpty)
        placements += operator -> operator.host
    }

    def placeIntermediates(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeIntermediates(_, consumer = false) }

      val host =
        //intermediate host/operator i.e. except for leaf node and root node
        if (!consumer && operator.dependencies.nonEmpty) {
          val valuesForHosts =
            //loop over all other nodes, except for nodes that are producers and consumers of events i.e leaf nodes and the root node
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _== host }) =>
              //collect latency of the node with the child nodes of the operator/host which is in consideration
              val propValues =
                operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }
              //select the minimum/maximum and associate with the host
              val min = minmax(optimizing, propValues)
              min -> host
            }

          if (valuesForHosts.isEmpty)
            throw new UnsupportedOperationException("not enough hosts")
          //select the minimum latency among all the hosts
          val (_, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }
          host
        }
        else
          operator.host

      placements += operator -> host
    }

    system.consumers foreach { placeProducersConsumers(_, consumer = true) }
    system.consumers foreach { placeIntermediates(_, consumer = true) }

    placements
  }

  private def placeOptimizingHeuristicB[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing)(
      merge: (T, T) => T): collection.Map[Operator, Host] = {
    val previousPlacements = mutable.Map.empty[Operator, mutable.Set[Host]]
    val placements = mutable.Map.empty[Operator, Host]

    def allOperators(operator: Operator, parent: Option[Operator]): Seq[(Operator, Option[Operator])] =
      (operator -> parent) +: (operator.dependencies flatMap { allOperators(_, Some(operator)) })

    val operators = system.consumers flatMap { allOperators(_, None) }
    operators foreach { case (operator, _) =>
      placements += operator -> operator.host
      previousPlacements += operator -> mutable.Set(operator.host)
    }

    @tailrec def placeOperators(): Unit = {
      val changed = operators map {
        case (operator, Some(parent)) if operator.dependencies.nonEmpty =>
          val valuesForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _ == host }) && !(previousPlacements(operator) contains host) =>
              merge(
                minmax(optimizing, operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }),
                selector(hostProps(placements(parent)), host)) -> host
            }

          val currentValue =
            merge(
              minmax(optimizing, operator.dependencies map { dependency =>
                selector(hostProps(placements(operator)), placements(dependency))
              }),
              selector(hostProps(placements(parent)), placements(operator)))

          val noPotentialPlacements =
            if (valuesForHosts.isEmpty) {
              if ((hostProps.keySet -- placements.values --previousPlacements(operator)).isEmpty)
                true
              else
                throw new UnsupportedOperationException("not enough hosts")
            }
            else
              false

          if (!noPotentialPlacements) {
            val (value, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }
            val changePlacement = value < currentValue
            if (changePlacement) {
              placements += operator -> host
              previousPlacements(operator) += host
            }

            changePlacement
          }
          else
            false

        case _ =>
          false
      }

      if (changed contains true)
        placeOperators()
    }

    placeOperators()

    placements
  }

  private def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]) = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  private def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U) = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }
}
