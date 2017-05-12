package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.data.Queries.Operator

/**
  * Created by monur.
  */
case class Violation(op: Operator, qos: QualityOfService)
