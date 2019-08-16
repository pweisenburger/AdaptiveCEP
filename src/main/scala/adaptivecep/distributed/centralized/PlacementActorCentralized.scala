package adaptivecep.distributed.centralized

import adaptivecep.data.Events._
import adaptivecep.data.Queries.{Operator => _, _}
import adaptivecep.distributed._
import adaptivecep.distributed.operator.{Host, NoHost, NodeHost, Operator}
import adaptivecep.graph.qos.MonitorFactory
import adaptivecep.privacy.Privacy.PrivacyContext
import akka.actor.{ActorRef, ActorSystem, Deploy, PoisonPill}
import akka.remote.RemoteScope
import rescala.default._

case class PlacementActorCentralized(actorSystem: ActorSystem,
                                     query: Query,
                                     publishers: Map[String, ActorRef],
                                     publisherHosts: Map[String, Host],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     bandwidthMonitorFactory: MonitorFactory,
                                     here: NodeHost,
                                     testHosts: Set[ActorRef],
                                     optimizeFor: String)(implicit val privacyContext: PrivacyContext)
  extends PlacementActorBase{

  def placeAll(map: Map[Operator, Host]): Unit ={
    map.foreach(pair => place(pair._1, pair._2))
    firstTimePlacement = false
    testHosts.foreach(host => host ! HostToNodeMap(hostToNodeMap))
    map.keys.foreach(operator => {
      if (operator.props != null) {
        val actorRef = propsActors(operator.props)
        val children = operator.dependencies
        val parent = parents(operator)
        if (parent.isDefined) {
          actorRef ! Parent(propsActors(parent.get.props))
        }
        children.length match {
          case 0 =>
          case 1 =>
            if (children.head.props != null) {
              actorRef ! Child1(propsActors(children.head.props))
              actorRef ! CentralizedCreated

            }
          case 2 =>
            if (children.head.props != null && children(1).props != null) {
              actorRef ! Child2(propsActors(children.head.props), propsActors(children(1).props))
              actorRef ! CentralizedCreated

            }
        }
      }
    })
    placement.set(map)
  }

  def place(operator: Operator, host: Host): Unit = {
    if(host != NoHost && operator.props != null){
      val moved = placement.now.contains(operator) && placement.now.apply(operator) != host
      if(moved) {
        val actor = propsActors(operator.props)
        log.info(s"killing operator ${actor.path}")
        actor ! Kill
      }
      if (moved || firstTimePlacement){
        val hostActor = host.asInstanceOf[NodeHost].actorRef
        val actor = propsActors(operator.props)
        println(s"\n Deploying ${actor.path} \n")
        val ref = actorSystem.actorOf(operator.props.withDeploy(Deploy(scope = RemoteScope(hostActor.path.address))))
        propsActors += operator.props -> ref
        hostToNodeMap += hostMap(hostActor) -> ref
        hostActor ! Node(ref)
        ref ! Controller(self)
      }
    }
  }
}
