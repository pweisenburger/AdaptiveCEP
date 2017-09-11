package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
import rescala._


/**
  * Created by monur.
  */
trait Demands {
  private val violatedDemandsVar: Var[Set[Violation]] = Var(Set.empty)
  private val adaptingVar: Var[Option[Set[Violation]]] = Var(None)
  private val fireAdaptationPlanned: Evt[Set[Violation]] = Evt[Set[Violation]]

  val violatedDemands: Signal[Set[Violation]] = violatedDemandsVar
  val adapting: Signal[Option[Set[Violation]]] = adaptingVar
  val adaptationPlanned: Event[Set[Violation]] = fireAdaptationPlanned

  def addViolatedDemand(violation: Violation): Unit = violatedDemandsVar.transform(_ + violation)
  def removeViolatedDemand(violation: Violation): Unit = violatedDemandsVar.transform(_ - violation)
  def fireAdaptationPlanned(violations: Set[Violation]): Unit = fireAdaptationPlanned fire violations
}
