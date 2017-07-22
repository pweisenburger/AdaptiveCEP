package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.qos.QualityOfService.Demand
import com.lambdarookie.eventscala.backend.system.traits.Operator

/**
  * Created by monur.
  */
case class Violation(op: Operator, qos: Demand)
