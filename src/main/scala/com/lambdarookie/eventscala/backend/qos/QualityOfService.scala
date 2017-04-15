package com.lambdarookie.eventscala.backend.qos

/**
  * Created by monur.
  */
trait QualityOfService
class Condition(protected val _isFulfilled: Boolean) extends QualityOfService {
  def isFulfilled = _isFulfilled
}
class Demand(_isFulfilled: Boolean) extends Condition(_isFulfilled) {
  private var condition: Option[Condition] = None
  private var isConditionFulfilled: Boolean = true

  def when(newCondition: Condition): Demand = {
    isConditionFulfilled = newCondition.isFulfilled
    condition = Some(newCondition)
    this
  }

  override def isFulfilled: Boolean = {
    if(isConditionFulfilled) _isFulfilled
    else true
  }
}