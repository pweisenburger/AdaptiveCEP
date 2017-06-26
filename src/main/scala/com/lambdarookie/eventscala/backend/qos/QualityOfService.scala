package com.lambdarookie.eventscala.backend.qos

/**
  * Created by monur.
  */
trait QualityOfService
class Condition(condition: Boolean) extends QualityOfService {
  def isFulfilled: Boolean = condition
}
class Demand(demand: Boolean) extends Condition(demand) {
  private var isConditionFulfilled: Boolean = true

  def when(newCondition: Condition): Demand = {
    isConditionFulfilled = newCondition.isFulfilled
    this
  }

  override def isFulfilled: Boolean = {
    if(isConditionFulfilled) demand
    else true
  }
}