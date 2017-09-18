package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.qos.QualityOfService.Violation
import rescala._


/**
  * Created by monur.
  */
trait Demands {
  private val violationsVar: Var[Set[Violation]] = Var(Set.empty)
  private val fireAdaptationPlanned: Evt[Set[Violation]] = Evt[Set[Violation]]
  private val waitingVar: Var[Set[Violation]] = Var(Set.empty)
  private val adaptingVar: Var[Option[Set[Violation]]] = Var(None)

  val violations: Signal[Set[Violation]] = violationsVar
  val adaptationPlanned: Event[Set[Violation]] = fireAdaptationPlanned
  val waiting: Signal[Set[Violation]] = waitingVar
  val adapting: Signal[Option[Set[Violation]]] = adaptingVar

  adaptationPlanned += { vs => waitingVar.transform(w => w ++ vs) }

  def addViolation(violation: Violation): Unit = violationsVar.transform(_ + violation)
  def removeViolation(violation: Violation): Unit = violationsVar.transform(_ - violation)
  def fireAdaptationPlanned(violations: Set[Violation]): Unit = fireAdaptationPlanned fire violations
  def startAdapting(): Unit = if (adapting.now.isEmpty) {
    val w: Set[Violation] = waiting.now
    waitingVar.transform(_ => Set.empty)
    adaptingVar.transform(_ => Some(w))
  }
  def stopAdapting(): Unit = adaptingVar.transform(_ => None)
}
