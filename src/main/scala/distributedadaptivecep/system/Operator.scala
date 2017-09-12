package distributedadaptivecep.system

import akka.actor.ActorRef

/**
  * Created by pratik_k on 5/26/2017.
  */
trait Host

object NoHost extends Host

trait Operator {
  val host: Host
  val dependencies: Seq[Operator]
  val optimumPath: Map[Any, Operator]
}

case class ActiveOperator(host: Host, parent: Option[ActiveOperator], tenativeOperators: Seq[Operator],
                          dependencies: Seq[Operator], actorRef: ActorRef, optimumPath: Map[Any, Operator]) extends Operator
case class TentativeOperator(host: Host, dependencies: Seq[Operator], neighbor: Operator, optimumPath: Map[Any, Operator]) extends Operator
case class NodeHost(id: Int, neighbors: Seq[Host]) extends Host