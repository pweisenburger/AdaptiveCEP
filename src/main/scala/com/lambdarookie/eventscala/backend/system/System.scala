package com.lambdarookie.eventscala.backend.system

import com.lambdarookie.eventscala.backend.data.Coordinate
import com.lambdarookie.eventscala.backend.qos.{Proximity, QualityOfService}
import rescala._

import scala.collection.SortedSet

/**
  * Created by monur.
  */
trait System extends CEPSystem with QoSSystem{

}

trait CEPSystem {
  val hosts: Signal[Set[Host]]
  val operators: Signal[Set[Operator]]
}
trait QoSSystem {
  val qos: Signal[Set[QualityOfService]]
  val demandViolated: Event[QualityOfService]
}
trait Host {
  val position: Coordinate
  val neighbors: Set[Host]

  def measureProximity(from: Host, to: Host): Int = ???

  def sortNeighborsByProximity: SortedSet[Host] = {
    val sorted = SortedSet[Host]()((x: Host, y: Host) =>
      Ordering[Int].compare(measureProximity(x, this), measureProximity(y, this)))
    sorted ++ neighbors
  }
}
trait Operator {
  val host: Host
  val inputs: Set[Operator]
  val outputs: Set[Operator]
}