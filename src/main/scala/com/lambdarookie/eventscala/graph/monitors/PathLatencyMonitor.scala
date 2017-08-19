package com.lambdarookie.eventscala.graph.monitors

import java.time._
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.backend.system.traits._
import com.lambdarookie.eventscala.data.Queries._

case class ChildLatencyRequest(time: Instant)
case class ChildLatencyResponse(childNode: ActorRef, requestTime: Instant)
case class PathLatency(childNode: ActorRef, path: Seq[Host], duration: Duration)

case class PathLatencyMonitor(interval: Int, logging: Boolean, testing: Boolean) extends NodeMonitor {

  val clock: Clock = Clock.systemDefaultZone

  var childNodeLatency: Option[Duration] = None
  var childNodePathToLatency: Option[(Seq[Host], Duration)] = None
  var childNode1Latency: Option[Duration] = None
  var childNode1PathToLatency: Option[(Seq[Host], Duration)] = None
  var childNode2Latency: Option[Duration] = None
  var childNode2PathToLatency: Option[(Seq[Host], Duration)] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val childNodes: Seq[ActorRef] = nodeData match {
      case lnd: LeafNodeData =>
        if (lnd.query.demands.collect{ case ld: LatencyDemand => ld }.nonEmpty && logging)
          println("LOG:\tLatency demands for leaf nodes are ignored, as leaf node latency is always considered 0.")
        Seq.empty
      case und: UnaryNodeData => Seq(und.childNode)
      case bnd: BinaryNodeData => Seq(bnd.childNode1, bnd.childNode2)
    }
    if (childNodes.nonEmpty) nodeData.context.system.scheduler.schedule(
      initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        childNodes.foreach(_ ! ChildLatencyRequest(clock.instant))
      })
  }

  override def onMessageReceive(message: Any, nodeData: NodeData): Unit = {
    val query: Query = nodeData.query
    val system: System = nodeData.system
    val self: ActorRef = nodeData.context.self
    val operator: Operator = system.nodesToOperators.now.apply(self)
    val host: Host = operator.host
    nodeData match {
      case _: LeafNodeData => message match {
        case ChildLatencyRequest(requestTime) =>
          nodeData.context.parent ! ChildLatencyResponse(self, requestTime)
          nodeData.context.parent ! PathLatency(self, Seq(host), Duration.ZERO)
      }
      case _: UnaryNodeData =>
        message match {
          case ChildLatencyRequest(time) => nodeData.context.parent ! ChildLatencyResponse(self, time)
          case ChildLatencyResponse(childNode, requestTime) =>
            childNodeLatency = if (testing) Some(host.lastLatencies(system.getHostByNode(childNode))._2.toDuration)
            else Some(Duration.between(requestTime, clock.instant).dividedBy(2))
            if (childNodePathToLatency.isDefined) {
              val pathToLatency: (Seq[Host], Duration) =
                (childNodePathToLatency.get._1 :+ host, childNodeLatency.get.plus(childNodePathToLatency.get._2))
              val latencyDemands: Set[LatencyDemand] =
                query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
              nodeData.context.parent ! PathLatency(self, pathToLatency._1, pathToLatency._2)
              if (logging && latencyDemands.nonEmpty)
                println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathToLatency._2.toMillis} ms. " +
                  s"(Calculated every $interval seconds.)")
              latencyDemands.foreach(ld =>
                if (isDemandNotMet(pathToLatency._2, ld)) system.fireDemandViolated(Violation(operator, ld)))
              childNodeLatency = None
              childNodePathToLatency = None
            }
          case PathLatency(_, path, duration) =>
            childNodePathToLatency = Some(path -> duration)
            if (childNodeLatency.isDefined) {
              val pathToLatency: (Seq[Host], Duration) =
                (childNodePathToLatency.get._1 :+ host, childNodeLatency.get.plus(childNodePathToLatency.get._2))
              val latencyDemands: Set[LatencyDemand] =
                query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
              nodeData.context.parent ! PathLatency(self, pathToLatency._1, pathToLatency._2)
              if (logging && latencyDemands.nonEmpty)
                println(s"LATENCY:\tEvents reach node `${nodeData.name}` through the path ${pathToLatency._1} after " +
                  s"${pathToLatency._2.toMillis} ms. (Calculated every $interval seconds.)")
              latencyDemands.foreach(ld =>
                if (isDemandNotMet(pathToLatency._2, ld)) system.fireDemandViolated(Violation(operator, ld)))
              childNodeLatency = None
              childNodePathToLatency = None
            }
        }
      case bnd: BinaryNodeData =>
        message match {
          case ChildLatencyRequest(time) => nodeData.context.parent ! ChildLatencyResponse(self, time)
          case ChildLatencyResponse(childNode, requestTime) =>
            val childHost: Host = system.getHostByNode(childNode)
            if (testing) childNode match {
              case bnd.childNode1 => childNode1Latency = Some(host.lastLatencies(childHost)._2.toDuration)
              case bnd.childNode2 => childNode2Latency = Some(host.lastLatencies(childHost)._2.toDuration)
            } else childNode match {
              case bnd.childNode1 => childNode1Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
              case bnd.childNode2 => childNode2Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
            }
            if (childNode1Latency.isDefined && childNode2Latency.isDefined &&
              childNode1PathToLatency.isDefined && childNode2PathToLatency.isDefined) {
              val path1ToLatency: (Seq[Host], Duration) =
                (childNode1PathToLatency.get._1 :+ host, childNode1Latency.get.plus(childNode1PathToLatency.get._2))
              val path2ToLatency: (Seq[Host], Duration) =
                (childNode2PathToLatency.get._1 :+ host, childNode2Latency.get.plus(childNode2PathToLatency.get._2))
              val slowPathToLatency: (Seq[Host], Duration) =
                if (path1ToLatency._2.compareTo(path2ToLatency._2) >= 0) path1ToLatency else path2ToLatency
              val latencyDemands: Set[LatencyDemand] =
                query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
              nodeData.context.parent ! PathLatency(self, slowPathToLatency._1, slowPathToLatency._2)
              if (logging && latencyDemands.nonEmpty)
                println(s"LATENCY:\tEvents reach node `${nodeData.name}` through the path ${slowPathToLatency._1} " +
                  s"after ${slowPathToLatency._2.toMillis} ms. (Calculated every $interval seconds.)")
              latencyDemands.foreach(ld =>
                if (isDemandNotMet(slowPathToLatency._2, ld)) system.fireDemandViolated(Violation(operator, ld)))
              childNode1Latency = None
              childNode2Latency = None
              childNode1PathToLatency = None
              childNode2PathToLatency = None
            }
          case PathLatency(childNode, path, duration) =>
            childNode match {
              case bnd.childNode1 => childNode1PathToLatency = Some(path -> duration)
              case bnd.childNode2 => childNode2PathToLatency = Some(path -> duration)
            }
            if (childNode1Latency.isDefined && childNode2Latency.isDefined &&
              childNode1PathToLatency.isDefined && childNode2PathToLatency.isDefined) {
              val path1ToLatency =
                (childNode1PathToLatency.get._1 :+ host, childNode1Latency.get.plus(childNode1PathToLatency.get._2))
              val path2ToLatency =
                (childNode2PathToLatency.get._1 :+ host, childNode2Latency.get.plus(childNode2PathToLatency.get._2))
              val slowPathToLatency: (Seq[Host], Duration) =
                if (path1ToLatency._2.compareTo(path2ToLatency._2) >= 0) path1ToLatency else path2ToLatency
              val latencyDemands: Set[LatencyDemand] =
                query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
              nodeData.context.parent ! PathLatency(self, slowPathToLatency._1, slowPathToLatency._2)
              if (logging && latencyDemands.nonEmpty) {
                println(s"LATENCY:\tEvents reach node `${nodeData.name}` through the path ${slowPathToLatency._1} " +
                  s"after ${slowPathToLatency._2.toMillis} ms. (Calculated every $interval seconds.)")
              }
              latencyDemands.foreach(ld =>
                if (isDemandNotMet(slowPathToLatency._2, ld)) system.fireDemandViolated(Violation(operator, ld)))
              childNode1Latency = None
              childNode2Latency = None
              childNode1PathToLatency = None
              childNode2PathToLatency = None
            }
      }

    }
  }

  private def isDemandNotMet(latency: Duration, ld: LatencyDemand): Boolean = {
    val met: Boolean = ld.booleanOperator match {
      case Equal =>        latency.compareTo(ld.timeSpan.toDuration) == 0
      case NotEqual =>     latency.compareTo(ld.timeSpan.toDuration) != 0
      case Greater =>      latency.compareTo(ld.timeSpan.toDuration) >  0
      case GreaterEqual => latency.compareTo(ld.timeSpan.toDuration) >= 0
      case Smaller =>      latency.compareTo(ld.timeSpan.toDuration) <  0
      case SmallerEqual => latency.compareTo(ld.timeSpan.toDuration) <= 0
    }
    !met
  }

  private def areConditionsMet(ld: LatencyDemand): Boolean = if(ld.conditions.exists(_.notFulfilled)) {
    if (logging)
      println("LOG:\tSome conditions for the latency demand are not met.")
    false
  } else true

}