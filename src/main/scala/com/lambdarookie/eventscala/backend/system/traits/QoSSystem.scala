package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.backend.qos.PathFinding.{Path, Priority}
import com.lambdarookie.eventscala.backend.qos.QoSUnits.{BitRate, TimeSpan, min}
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
import com.lambdarookie.eventscala.data.Queries.Query
import rescala._

/**
  * Created by Ders.
  */
trait QoSSystem {
  protected val logging: Boolean

  val priority: Priority

  def decideAdaptation(violations: Set[Violation]): Set[Violation]


  private var paths: Set[Path] = Set.empty

  private val queriesVar: Var[Set[Query]] = Var(Set.empty)
  private val fireDemandsViolated: Evt[Set[Violation]] = Evt[Set[Violation]]
  private val adaptingVar: Var[Option[Set[Violation]]] = Var(None)
  private val adaptingHelper: Signal[Option[Set[Violation]]] = Signal {
    if (queriesVar().exists(_.adapting().nonEmpty))
      Some(queriesVar().flatMap(_.adapting()).flatten)
    else
      None
  }

  protected val demandsViolated: Event[Set[Violation]] = fireDemandsViolated

  val violations: Signal[Set[Violation]] = Signal{ queriesVar().flatMap(_.violations()) }
  val waiting: Signal[Set[Violation]] = Signal { queriesVar().flatMap(_.waiting()) }
  val adapting: Signal[Option[Set[Violation]]] = adaptingVar


  demandsViolated += { vs =>
    val query: Query = vs.head.operator.query
    query.addViolations(vs)
    val adaptationPlanned: Set[Violation] = decideAdaptation(vs)
    if (adaptationPlanned.nonEmpty) query.fireAdaptationPlanned(adaptationPlanned)
  }

  waiting.change += { diff =>
    if (diff.from.get.isEmpty && diff.to.get.nonEmpty) {
      if (logging) println(s"ADAPTATION:\t${diff.to.get} waiting adaptation")
      if (adaptingHelper.now.isEmpty) diff.to.get.map(_.operator.query).foreach(_.startAdapting())
    }
  }

  adaptingHelper.change += { diff =>
    val from: Option[Set[Violation]] = diff.from.get
    val to: Option[Set[Violation]] = diff.to.get
    if (from.isEmpty && to.isEmpty) {
      adaptingVar.transform(_ => None)
      if (logging) println(s"ADAPTATION:\tSystem is done adapting")
    } else if (from.isEmpty && to.nonEmpty) {
      if (logging) println(s"ADAPTATION:\tSystem is adapting to violations: ${to.get}")
      adaptingVar.transform(_ => to)
      to.get.foreach(_.operator.query.stopAdapting())
    } else if (from.nonEmpty && to.isEmpty) {
      adaptingVar.transform(_ => None)
      if (logging) println(s"ADAPTATION:\tSystem is done adapting")
      waiting.now.map(_.operator.query).foreach(_.startAdapting())
    }
  }


  private def updatePaths(path: Seq[Host]): Unit = {
    val path1 = path.init
    var path2 = path.tail
    paths ++ path1.flatMap { h1 =>
      val out: Set[Path] = path2.map { h2 => Path(h1, h2, path2.span(_ != h2)._1) }.toSet
      path2 = path2.tail
      out
    }
  }

  private[backend] def addQuery(query: Query): Unit = queriesVar.transform(x => x + query)

  private[backend] def fireDemandsViolated(violations: Set[Violation]): Unit =  fireDemandsViolated fire violations

  def getLatencyAndUpdatePaths(from: Host, to: Host, through: Option[Host] = None): TimeSpan =
    if (through.nonEmpty && through.get != to) {
      getLatencyAndUpdatePaths(from, through.get) + getLatencyAndUpdatePaths(through.get, to)
    } else {
      val found: Set[Path] = paths collect { case p@Path(`from`, `to`, _) => p }
      if (found.size == 1) {
        found.head.latency
      } else {
        if (found.size > 1) paths --= found // If there are duplicates it is en error. Remove them
        val bestPath: Path = priority.choosePath(from, to, paths)
        updatePaths(bestPath.toSeq)
        bestPath.latency
      }
    }

  def getBandwidthAndUpdatePaths(from: Host, to: Host, through: Option[Host] = None): BitRate =
    if (through.nonEmpty && through.get != to) {
      min(getBandwidthAndUpdatePaths(from, through.get), getBandwidthAndUpdatePaths(through.get, to))
    } else {
      val found: Set[Path] = paths collect { case p@Path(`from`, `to`, _) => p }
      if (found.size == 1) {
        found.head.bandwidth
      } else {
        if (found.size > 1) paths --= found // If there are duplicates it is en error. Remove them
        val bestPath: Path = priority.choosePath(from, to, paths)
        updatePaths(bestPath.toSeq)
        bestPath.bandwidth
      }
    }

  def getThroughputAndUpdatePaths(from: Host, to: Host, through: Option[Host] = None): BitRate =
    if (through.nonEmpty && through.get != to) {
      min(getThroughputAndUpdatePaths(from, through.get), getThroughputAndUpdatePaths(through.get, to))
    } else {
      val found: Set[Path] = paths collect { case p@Path(`from`, `to`, _) => p }
      if (found.size == 1) {
        found.head.throughput
      } else {
        if (found.size > 1) paths --= found // If there are duplicates it is en error. Remove them
        val bestPath: Path = priority.choosePath(from, to, paths)
        updatePaths(bestPath.toSeq)
        bestPath.throughput
      }
    }

  def forgetPaths(): Unit = paths = Set.empty
}
