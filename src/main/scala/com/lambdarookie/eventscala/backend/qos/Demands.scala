package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService.Demand
import rescala._


/**
  * Created by monur.
  */
trait Demands {
  def violatedDemands: Signal[Set[Demand]] = violatedDemandsVar
//  def adapting: Signal[Option[Set[Demand]]]
//  def adaptationPlanned: Event[Set[Demand]]
//  def delayAdaptation(delay: Event[TimeSpan]): Unit

  private val violatedDemandsVar: Var[Set[Demand]] = Var(Set.empty)

  def addViolatedDemand(demand: Demand): Unit = violatedDemandsVar.transform(x => x + demand)
}
