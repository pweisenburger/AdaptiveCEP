package com.lambdarookie.eventscala.simulation

import com.lambdarookie.eventscala.simulation.Simulation._
import com.lambdarookie.eventscala.system.{Host, Operator, System}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.Random

object Simulation {
  case class HostId(id: Int) extends Host

  case class HostProps(latency: Seq[(Host, ContinuousBoundedValue[Duration])]) {
    def advance = HostProps(latency map { case (host, latency) => (host, latency.advance) })
  }

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing
}

class Simulation(system: System) {
  val random = new Random(0)

  val nodeCount = 25

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


  private def avgDuration(durations: Seq[Duration]): Duration =
    if (durations.isEmpty)
      Duration.Zero
    else
      durations.foldLeft[Duration](Duration.Zero) { _ + _ } / durations.size

  private def latencySelector(props: HostProps, host: Host): Duration =
    (props.latency collectFirst { case (`host`, latency) => latency }).get.value

  private var hostProps: Map[Host, HostProps] = {
    val hostIds  = 0 until nodeCount map HostId
    (hostIds map { id =>
      id -> HostProps(hostIds collect {
        case otherId if otherId != id => otherId -> latency()
      })
    }).toMap
  }

  private var time = Duration.Zero


  def currentTime = time

  def advance(): Unit = {
    hostProps = (hostProps mapValues { _.advance }).view.force
    time += stepDuration
  }

  def measureLatency: Duration =
    measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avgDuration } { _.host }

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
      else
        minmax(optimizing, operator.dependencies map { dependentOperator =>
          merge(measure(dependentOperator), selector(hostProps(host(operator)), host(dependentOperator)))
        })

    avg(system.consumers map measure)
  }

  def placeSequentially(): Unit = {
    def allOperators(operator: Operator): Seq[Operator] =
      operator +: (operator.dependencies flatMap allOperators)
    var nodeIndex = 0
    def placeOperator(operator: Operator): Unit = {
      if (nodeIndex >= nodeCount)
        throw new UnsupportedOperationException("not enough hosts")

      system place (operator, HostId(nodeIndex))
      nodeIndex += 1

      operator.dependencies foreach placeOperator
    }

    system.consumers foreach placeOperator
  }

  def placeOptimizingLatency(): Unit = {
    val measureLatency = measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avgDuration } _

    val placementsA = placeOptimizingHeuristicA(latencySelector, Minimizing)
    val durationA = measureLatency { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencySelector, Minimizing) { _ + _ }
    val durationB = measureLatency { placementsB(_) }

    (if (durationA < durationB) placementsA else placementsB) foreach { case (operator, host) =>
      system place (operator, host)
    }
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
        if (!consumer && operator.dependencies.nonEmpty) {
          val durationsForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _== host }) =>
              val propValues =
                operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }

              minmax(optimizing, propValues) -> host
            }

          if (durationsForHosts.isEmpty)
            throw new UnsupportedOperationException("not enough hosts")

          val (duration, host) = minmaxBy(optimizing, durationsForHosts) { case (duration, _) => duration }
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
    val placements = mutable.Map.empty[Operator, Host]

    def allOperators(operator: Operator, parent: Option[Operator]): Seq[(Operator, Option[Operator])] =
      (operator -> parent) +: (operator.dependencies flatMap { allOperators(_, Some(operator)) })

    val operators = system.consumers flatMap { allOperators(_, None) }
    operators foreach { case (operator, _) => placements += operator -> operator.host }

    @tailrec def placeOperators(): Unit = {
      val changed = operators map {
        case (operator, Some(parent)) if operator.dependencies.nonEmpty =>
          val durationsForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists {
              _ == host
            }) =>
              merge(
                minmax(optimizing, operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }),
                selector(hostProps(placements(parent)), host)) -> host
            }

          val currentDuration =
            merge(
              minmax(optimizing, operator.dependencies map { dependency =>
                selector(hostProps(placements(operator)), placements(dependency))
              }),
              selector(hostProps(placements(parent)), placements(operator)))

          if (durationsForHosts.isEmpty)
            throw new UnsupportedOperationException("not enough hosts")

          val (duration, host) = minmaxBy(optimizing, durationsForHosts) { case (duration, _) => duration }

          val changePlacement = duration < currentDuration
          if (changePlacement)
            placements += operator -> host

          changePlacement

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
