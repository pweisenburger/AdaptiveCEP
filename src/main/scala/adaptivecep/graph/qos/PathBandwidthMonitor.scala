package adaptivecep.graph.qos

import java.util.concurrent.TimeUnit

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Events.{CostReport, HostPropsResponse}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.actor.{ActorRef, Cancellable}
import adaptivecep.data.Queries._

case object ChildBandwidthRequest
case class ChildBandwidthResponse(childNode: ActorRef, bandwidth: Double)
case class PathBandwidth(childNode: ActorRef, bandwidth: Double)

trait PathBandwidthMonitor {

  var costs: Map[ActorRef, Cost] = Map.empty[ActorRef, Cost] withDefaultValue(Cost(Duration.Inf, 0))

  def isRequirementNotMet(bandwidth: Double, br: BandwidthRequirement): Boolean = {
    val met: Boolean = br.operator match {
      case Equal =>        bandwidth.compareTo(br.bandwidth) == 0
      case NotEqual =>     bandwidth.compareTo(br.bandwidth) != 0
      case Greater =>      bandwidth.compareTo(br.bandwidth) >  0
      case GreaterEqual => bandwidth.compareTo(br.bandwidth) >= 0
      case Smaller =>      bandwidth.compareTo(br.bandwidth) <  0
      case SmallerEqual => bandwidth.compareTo(br.bandwidth) <= 0
    }
    !met
  }

}

case class PathBandwidthLeafNodeMonitor() extends PathBandwidthMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = {
    if (nodeData.requirements.collect{ case br: BandwidthRequirement => br }.nonEmpty){}
      //println("PROBLEM:\tLatency requirements for leaf nodes are ignored, as leaf node latency is always considered 0.")
  }

  override def onMessageReceive(message: Any, nodeData: LeafNodeData): Unit = {
    message match {
      case ChildBandwidthRequest =>
        if(costs.contains(nodeData.parent)){
          nodeData.parent ! ChildBandwidthResponse(nodeData.context.self, costs(nodeData.parent).bandwidth)
        }
        nodeData.parent ! PathBandwidth(nodeData.context.self, Double.MaxValue)
      case CostReport(costMap) => costs = costMap
      case _ =>
    }
  }
}

case class PathBandwidthUnaryNodeMonitor(interval: Int, logging: Boolean)
  extends PathBandwidthMonitor with UnaryNodeMonitor {

  var scheduledTask: Cancellable = null
  var met: Boolean = true
  var bandwidthForMonitoring: Option[Double] = None
  var childNode: ActorRef = null
  var childNodeBandwidth: Option[Double] = None
  var childNodePathBandwidth: Option[Double] = None


  override def onCreated(nodeData: UnaryNodeData): Unit = {
    childNode = nodeData.childNode
    if(scheduledTask == null) {
      scheduledTask = nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
        runnable = () => childNode ! ChildBandwidthRequest)
    }
  }

  override def onMessageReceive(message: Any, nodeData: UnaryNodeData): Unit = {
    val callbackNodeData: NodeData = NodeData(nodeData.name, nodeData.requirements, nodeData.context)
    val bandwidthRequirements: Set[BandwidthRequirement] =
      nodeData.requirements.collect { case br: BandwidthRequirement => br }
    message match {
      case CostReport(costMap) => costs = costMap
      case ChildBandwidthRequest =>
        if(costs.contains(nodeData.parent)){
          nodeData.parent ! ChildBandwidthResponse(nodeData.context.self, costs(nodeData.parent).bandwidth)
        }
        //nodeData.parent ! ChildBandwidthResponse(nodeData.context.self, costs(nodeData.parent).bandwidth)
      case ChildBandwidthResponse(_, bandwidth) =>
        childNodeBandwidth = Some(bandwidth)
        if (childNodePathBandwidth.isDefined) {
          val pathBandwidth: Double = Math.min(childNodeBandwidth.get, childNodePathBandwidth.get)
          bandwidthForMonitoring = Some(pathBandwidth)
          nodeData.parent ! PathBandwidth(nodeData.context.self, pathBandwidth)
          if (logging && bandwidthRequirements.nonEmpty)
            println(
              s"BANDWIDTH:\tBandwidth to node `${nodeData.name}` is $pathBandwidth. " +
              s"(Calculated every $interval seconds.)")
          met = true
          bandwidthRequirements.foreach(
            br =>{
              if (isRequirementNotMet(pathBandwidth, br)) {
                br.callback(callbackNodeData)
                met = false
              }
            })
          childNodeBandwidth = None
          childNodePathBandwidth = None
        }
      case PathBandwidth(_, bandwidth) =>
        childNodePathBandwidth = Some(bandwidth)
        if (childNodeBandwidth.isDefined) {
          val pathBandwidth: Double = Math.min(childNodeBandwidth.get, childNodePathBandwidth.get)
          bandwidthForMonitoring = Some(pathBandwidth)
          nodeData.parent ! PathBandwidth(nodeData.context.self, pathBandwidth)
          if (logging && bandwidthRequirements.nonEmpty)
            println(
              s"BANDWIDTH:\tBandwidth to node `${nodeData.name}` is $pathBandwidth. " +
              s"(Calculated every $interval seconds.)")
          met = true
          bandwidthRequirements.foreach(
            br =>
              if (isRequirementNotMet(pathBandwidth, br)){
                br.callback(callbackNodeData)
                met = false
              }
            )
          childNodeBandwidth = None
          childNodePathBandwidth = None
        }
      case _ =>
    }
  }

}

case class PathBandwidthBinaryNodeMonitor(interval: Int, logging: Boolean)
  extends PathBandwidthMonitor with BinaryNodeMonitor {

  var met: Boolean = true
  var bandwidthForMonitoring: Option[Double] = None
  var scheduledTask: Cancellable = null
  var childNode1: ActorRef = _
  var childNode2: ActorRef = _
  var childNode1Bandwidth: Option[Double] = None
  var childNode2Bandwidth: Option[Double] = None
  var childNode1PathBandwidth: Option[Double] = None
  var childNode2PathBandwidth: Option[Double] = None

  override def onCreated(nodeData: BinaryNodeData): Unit = {
    childNode1 = nodeData.childNode1
    childNode2 = nodeData.childNode2
    if(scheduledTask == null) {
      scheduledTask = nodeData.context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.MILLISECONDS),
        runnable = () => {
          childNode1 ! ChildBandwidthRequest
          childNode2 ! ChildBandwidthRequest
        })
    }
  }

  override def onMessageReceive(message: Any, nodeData: BinaryNodeData): Unit = {
    val callbackNodeData: NodeData = NodeData(nodeData.name, nodeData.requirements, nodeData.context)
    val bandwidthRequirements: Set[BandwidthRequirement] =
      nodeData.requirements.collect { case br: BandwidthRequirement => br }
    message match {
      case CostReport(costMap) => costs = costMap
      case ChildBandwidthRequest =>
        if(costs.contains(nodeData.parent)){
          nodeData.parent ! ChildBandwidthResponse(nodeData.context.self, costs(nodeData.parent).bandwidth)
        }
      case ChildBandwidthResponse(childNode, bandwidth) =>
        childNode match {
          case nodeData.childNode1 =>
            childNode1Bandwidth = Some(bandwidth)
          case nodeData.childNode2 =>
            childNode2Bandwidth = Some(bandwidth)
          case _ =>
        }
        if (childNode1Bandwidth.isDefined &&
          childNode2Bandwidth.isDefined &&
          childNode1PathBandwidth.isDefined &&
          childNode2PathBandwidth.isDefined) {
          val pathBandwidth1 = Math.min(childNode1Bandwidth.get, childNode1PathBandwidth.get)
          val pathBandwidth2 = Math.min(childNode2Bandwidth.get, childNode2PathBandwidth.get)
          if (pathBandwidth1.compareTo(pathBandwidth2) <= 0) {
            bandwidthForMonitoring = Some(pathBandwidth1)
            nodeData.parent ! PathBandwidth(nodeData.context.self, pathBandwidth1)
            if (logging && bandwidthRequirements.nonEmpty)
              println(
                s"BANDWIDTH:\tBandwidth to node `${nodeData.name}` is $pathBandwidth1. " +
                s"(Calculated every $interval seconds.)")
            met = true
            bandwidthRequirements.foreach(br => if (isRequirementNotMet(pathBandwidth1, br)){
              br.callback(callbackNodeData)
              met = false
            })
          } else {
            bandwidthForMonitoring = Some(pathBandwidth2)
            nodeData.parent ! PathBandwidth(nodeData.context.self, pathBandwidth2)
            if (logging && bandwidthRequirements.nonEmpty)
              println(
                s"BANDWIDTH:\tBandwidth to node `${nodeData.name}` is $pathBandwidth2. " +
                s"(Calculated every $interval seconds.)")
            met = true
            bandwidthRequirements.foreach(br => if (isRequirementNotMet(pathBandwidth2, br)){
              br.callback(callbackNodeData)
              met = false
            })
          }
          childNode1Bandwidth = None
          childNode2Bandwidth = None
          childNode1PathBandwidth = None
          childNode2PathBandwidth = None
        }
      case PathBandwidth(childNode, bandwidth) =>
        childNode match {
          case nodeData.childNode1 => childNode1PathBandwidth = Some(bandwidth)
          case nodeData.childNode2 => childNode2PathBandwidth = Some(bandwidth)
          case _ =>
        }
        if (childNode1Bandwidth.isDefined &&
          childNode2Bandwidth.isDefined &&
          childNode1PathBandwidth.isDefined &&
          childNode2PathBandwidth.isDefined) {
          val pathBandwidth1 = Math.min(childNode1Bandwidth.get, childNode1PathBandwidth.get)
          val pathBandwidth2 = Math.min(childNode2Bandwidth.get, childNode2PathBandwidth.get)
          if (pathBandwidth1.compareTo(pathBandwidth2) <= 0) {
            bandwidthForMonitoring = Some(pathBandwidth1)
            nodeData.parent ! PathBandwidth(nodeData.context.self, pathBandwidth1)
            if (logging && bandwidthRequirements.nonEmpty)
              println(
                s"BANDWIDTH:\tBandwidth to node `${nodeData.name}` is $pathBandwidth1. " +
                  s"(Calculated every $interval seconds.)")
            met = true
            bandwidthRequirements.foreach(br =>
              if (isRequirementNotMet(pathBandwidth1, br)){
                br.callback(callbackNodeData)
                met = false
              }
            )
          } else {
            bandwidthForMonitoring = Some(pathBandwidth2)
            nodeData.parent ! PathBandwidth(nodeData.context.self, pathBandwidth2)
            if (logging && nodeData.requirements.collect { case br: BandwidthRequirement => br }.nonEmpty)
              println(
                s"BANDWIDTH:\tBandwidth to node `${nodeData.name}` is $pathBandwidth2. " +
                  s"(Calculated every $interval seconds.)")
            met = true
            nodeData.requirements.collect { case br: BandwidthRequirement => br }.foreach(br =>
              if (isRequirementNotMet(pathBandwidth2, br)){
                br.callback(callbackNodeData)
                met = false
              }
            )
          }
          childNode1Bandwidth = None
          childNode2Bandwidth = None
          childNode1PathBandwidth = None
          childNode2PathBandwidth = None
        }
      case _ =>
    }

  }

}

case class PathBandwidthMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = PathBandwidthLeafNodeMonitor()
  override def createUnaryNodeMonitor: UnaryNodeMonitor = PathBandwidthUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = PathBandwidthBinaryNodeMonitor(interval, logging)

}
