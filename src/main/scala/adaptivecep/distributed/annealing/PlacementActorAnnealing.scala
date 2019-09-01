package adaptivecep.distributed.annealing

import adaptivecep.data.Events._
import adaptivecep.data.Queries.{Operator => _, _}
import adaptivecep.distributed._
import adaptivecep.distributed.operator._
import adaptivecep.graph.qos.MonitorFactory
import adaptivecep.privacy.PrivacyContext
import akka.actor.{ActorRef, ActorSystem, Deploy}
import akka.remote.RemoteScope
import rescala.default._


case class PlacementActorAnnealing(actorSystem: ActorSystem,
                                   query: Query,
                                   publishers: Map[String, ActorRef],
                                   publisherHosts: Map[String, Host],
                                   frequencyMonitorFactory: MonitorFactory,
                                   latencyMonitorFactory: MonitorFactory,
                                   bandwidthMonitorFactory: MonitorFactory,
                                   here: NodeHost,
                                   testHosts: Set[ActorRef],
                                   optimizeFor: String)(implicit val privacyContext: PrivacyContext)
  extends PlacementActorBase {

  def placeAll(map: Map[Operator, Host]): Unit ={
    map.foreach(pair => place(pair._1, pair._2))
    map.keys.foreach(operator => {
      if (operator.props != null) {
        val actorRef = propsActors(operator.props)
        val children = operator.dependencies
        children.length match {
          case 0 =>
          case 1 =>
            if (children.head.props != null) {
              map(operator).asInstanceOf[NodeHost].actorRef ! ChildHost1(map(propsOperators(children.head.props)).asInstanceOf[NodeHost].actorRef)
              actorRef ! Child1(propsActors(children.head.props))
              actorRef ! CentralizedCreated

            }
          case 2 =>
            if (children.head.props != null && children(1).props != null) {
              map(operator).asInstanceOf[NodeHost].actorRef ! ChildHost2(map(propsOperators(children.head.props)).asInstanceOf[NodeHost].actorRef, map(propsOperators(children(1).props)).asInstanceOf[NodeHost].actorRef)
              actorRef ! Child2(propsActors(children.head.props), propsActors(children(1).props))
              actorRef ! CentralizedCreated

            }
        }
        val parent = parents(operator)
        if (parent.isDefined) {
          map(operator).asInstanceOf[NodeHost].actorRef ! ParentHost(map(propsOperators(parent.get.props)).asInstanceOf[NodeHost].actorRef, propsActors(parent.get.props))
        }
      }
    })
    consumers.now.foreach(consumer => map(consumer).asInstanceOf[NodeHost].actorRef ! ChooseTentativeOperators(Set.empty[NodeHost]))
    placement.set(map)
  }

  def place(operator: Operator, host: Host): Unit = {
    if(host != NoHost && operator.props != null){
      val moved = placement.now.contains(operator) && placement.now.apply(operator) != host
      if(moved) {
        propsActors(operator.props) ! Kill
      }
      if (moved || placement.now.size < operators.now.size){
        val hostActor = host.asInstanceOf[NodeHost].actorRef
        val ref = actorSystem.actorOf(operator.props.withDeploy(Deploy(scope = RemoteScope(hostActor.path.address))))
        hostActor ! SetActiveOperator(operator.props)
        hostActor ! Node(ref)
        ref ! Controller(hostActor)
        propsActors += operator.props -> ref
      }
    }
  }
}
