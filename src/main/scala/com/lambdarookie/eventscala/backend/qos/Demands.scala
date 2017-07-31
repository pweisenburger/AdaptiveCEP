package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
import rescala._


/**
  * Created by monur.
  */
trait Demands {
  protected val violatedDemandsVar: Var[Set[Violation]] = Var(Set.empty)

  val violatedDemands: Signal[Set[Violation]]
//  def adapting: Signal[Option[Set[Demand]]]
//  def adaptationPlanned: Event[Set[Demand]]
//  def delayAdaptation(delay: Event[TimeSpan]): Unit


  def addViolatedDemand(violation: Violation): Unit = violatedDemandsVar.transform(x => x + violation)
}
