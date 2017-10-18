package com.lambdarookie.eventscala.backend.system.traits

import com.lambdarookie.eventscala.backend.qos.QualityOfService.Adaptation
import rescala._

/**
  * Created by monur.
  */
sealed trait System extends CEPSystem with QoSSystem

abstract class SystemImpl(val strategy: System => Event[Adaptation]) extends System {
  strategy(this) += { adaptation => replaceOperators(adaptation.assignments) }
  hosts.changed += { _ => forgetPaths() }
}











