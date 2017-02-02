package com.scalarookie.eventscala.backend.system

import com.scalarookie.eventscala.backend.data.Coordinate
import com.scalarookie.eventscala.backend.qos.QualityOfService
import rescala._

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
}
trait Operator {
  val host: Host
  val inputs: Set[Operator]
  val outputs: Set[Operator]
}