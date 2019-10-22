package adaptivecep.distributed.operator

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Queries.Requirement
import akka.actor.{ActorRef, Props}
import rescala.default.Signal
import rescala.default.Event

import scala.concurrent.duration.Duration

sealed trait Host

object NoHost extends Host

trait Operator {
  val props: Props
  val dependencies: Seq[Operator]
}

trait  CEPSystem {
  val hosts: Signal[Set[NodeHost]]
  val operators: Signal[Set[Operator]]
  val placement: Signal[Map[Operator, Host]]
}

trait QoSSystem{
  val qos: Signal[Map[Host, HostProps]] //aggregierte kosten
  val demandViolated: Event[Set[Requirement]] // currently the node reports this via Requirements not met (could be changed to firing an event)
}

trait System extends CEPSystem with QoSSystem

case class HostProps(latency: Seq[(Host, Duration)], bandwidth: Seq[(Host, Double)])

case class ActiveOperator(props: Props, dependencies: Seq[Operator]) extends Operator
case class TentativeOperator(props: Props, dependencies: Seq[Operator]) extends Operator

case class NodeHost(actorRef: ActorRef) extends Host

trait HostDecorator extends Host {
  def host: Host
}

case class TrustedHost(host: Host) extends HostDecorator





