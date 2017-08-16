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
case class PathLatency(childNode: ActorRef, duration: Duration)

case class PathLatencyMonitor(interval: Int, logging: Boolean, testing: Boolean) extends NodeMonitor {

  val clock: Clock = Clock.systemDefaultZone

  var childNodeLatency: Option[Duration] = None
  var childNodePathLatency: Option[Duration] = None
  var childNode1Latency: Option[Duration] = None
  var childNode1PathLatency: Option[Duration] = None
  var childNode2Latency: Option[Duration] = None
  var childNode2PathLatency: Option[Duration] = None

  override def onCreated(nodeData: NodeData): Unit = {
    val childNodes: Seq[ActorRef] = nodeData match {
      case lnd: LeafNodeData =>
        if(lnd.query.demands.collect{ case ld: LatencyDemand => ld }.nonEmpty && logging)
          println("LOG:\tLatency demands for leaf nodes are ignored, as leaf node latency is always considered 0.")
        Seq.empty
      case und: UnaryNodeData => Seq(und.childNode)
      case bnd: BinaryNodeData => Seq(bnd.childNode1, bnd.childNode2)
    }
    if(childNodes.nonEmpty) nodeData.context.system.scheduler.schedule(
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
    nodeData match {
      case _: LeafNodeData => message match {
        case ChildLatencyRequest(requestTime) =>
          nodeData.context.parent ! ChildLatencyResponse(self, requestTime)
          nodeData.context.parent ! PathLatency(self, Duration.ZERO)
      }
      case _: UnaryNodeData =>
        message match {
          case ChildLatencyRequest(time) => nodeData.context.parent ! ChildLatencyResponse(self, time)
          case ChildLatencyResponse(childNode, requestTime) =>
            childNodeLatency = if(testing) Some(operator.host.lastLatencies(system.getHostByNode(childNode)).toDuration)
            else Some(Duration.between(requestTime, clock.instant).dividedBy(2))
            if (childNodePathLatency.isDefined) {
              val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
              val latencyDemands: Set[LatencyDemand] =
                query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
              nodeData.context.parent ! PathLatency(self, pathLatency)
              if (logging && latencyDemands.nonEmpty)
                println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathLatency.toMillis} ms. " +
                  s"(Calculated every $interval seconds.)")
              latencyDemands.foreach(ld =>
                if (isDemandNotMet(pathLatency, ld)) system.fireDemandViolated(Violation(operator, ld)))
              childNodeLatency = None
              childNodePathLatency = None
            }
          case PathLatency(_, duration) =>
            childNodePathLatency = Some(duration)
            if (childNodeLatency.isDefined) {
              val pathLatency: Duration = childNodeLatency.get.plus(childNodePathLatency.get)
              val latencyDemands: Set[LatencyDemand] =
                query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
              nodeData.context.parent ! PathLatency(self, pathLatency)
              if (logging && latencyDemands.nonEmpty)
                println(s"LATENCY:\tEvents reach node `${nodeData.name}` after ${pathLatency.toMillis} ms. " +
                  s"(Calculated every $interval seconds.)")
              latencyDemands.foreach(ld =>
                if (isDemandNotMet(pathLatency, ld)) system.fireDemandViolated(Violation(operator, ld)))
              childNodeLatency = None
              childNodePathLatency = None
            }
        }
      case bnd: BinaryNodeData =>
        message match {
        case ChildLatencyRequest(time) => nodeData.context.parent ! ChildLatencyResponse(self, time)
        case ChildLatencyResponse(childNode, requestTime) =>
          val childHost: Host = system.getHostByNode(childNode)
          if (testing) childNode match {
            case bnd.childNode1 => childNode1Latency = Some(operator.host.lastLatencies(childHost).toDuration)
            case bnd.childNode2 => childNode2Latency = Some(operator.host.lastLatencies(childHost).toDuration)
          } else childNode match {
            case bnd.childNode1 => childNode1Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
            case bnd.childNode2 => childNode2Latency = Some(Duration.between(requestTime, clock.instant).dividedBy(2))
          }
          if (childNode1Latency.isDefined && childNode2Latency.isDefined &&
            childNode1PathLatency.isDefined && childNode2PathLatency.isDefined) {
            val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
            val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
            val latencyDemands: Set[LatencyDemand] =
              query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
            if (pathLatency1.compareTo(pathLatency2) >= 0) nodeData.context.parent ! PathLatency(self, pathLatency1)
            else nodeData.context.parent ! PathLatency(self, pathLatency2)
            if (logging && latencyDemands.nonEmpty)
              println(s"LATENCY:\tEvents reach node `${nodeData.name}` after " +
                s"${math.max(pathLatency1.toMillis, pathLatency2.toMillis)} ms. (Calculated every $interval seconds.)")
            latencyDemands.foreach(ld =>
              if (isDemandNotMet(pathLatency1, ld)) system.fireDemandViolated(Violation(operator, ld)))
            childNode1Latency = None
            childNode2Latency = None
            childNode1PathLatency = None
            childNode2PathLatency = None
          }
        case PathLatency(childNode, duration) =>
          childNode match {
            case bnd.childNode1 => childNode1PathLatency = Some(duration)
            case bnd.childNode2 => childNode2PathLatency = Some(duration)
          }
          if (childNode1Latency.isDefined && childNode2Latency.isDefined &&
            childNode1PathLatency.isDefined && childNode2PathLatency.isDefined) {
            val pathLatency1 = childNode1Latency.get.plus(childNode1PathLatency.get)
            val pathLatency2 = childNode2Latency.get.plus(childNode2PathLatency.get)
            val latencyDemands: Set[LatencyDemand] =
              query.demands.collect { case ld: LatencyDemand if areConditionsMet(ld) => ld }
            if (pathLatency1.compareTo(pathLatency2) >= 0) nodeData.context.parent ! PathLatency(self, pathLatency1)
            else nodeData.context.parent ! PathLatency(self, pathLatency2)
            if (logging && latencyDemands.nonEmpty)
              println(s"LATENCY:\tEvents reach node `${nodeData.name}` after " +
                s"${math.max(pathLatency1.toMillis, pathLatency2.toMillis)} ms. (Calculated every $interval seconds.)")
            latencyDemands.foreach(ld =>
              if (isDemandNotMet(pathLatency1, ld)) system.fireDemandViolated(Violation(operator, ld)))
            childNode1Latency = None
            childNode2Latency = None
            childNode1PathLatency = None
            childNode2PathLatency = None
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
    if(logging)
      println("LOG:\tSome conditions for the latency demand are not met.")
    false
  } else true

}