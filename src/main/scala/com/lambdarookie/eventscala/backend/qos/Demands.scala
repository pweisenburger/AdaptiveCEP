package com.lambdarookie.eventscala.backend.qos

import rescala._

import scala.concurrent.duration.Duration

/**
  * Created by monur.
  */
trait Demands {
  def violatedDemands: Signal[Set[QualityOfService]]
  def adapting: Signal[Option[Set[QualityOfService]]]
  def adaptationPlanned: Event[Set[QualityOfService]]
  def delayAdaptation(delay: Event[Duration]): Unit
}
