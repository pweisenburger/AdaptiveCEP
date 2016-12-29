package com.scalarookie.eventscala.qos

import akka.actor.ActorContext
import com.scalarookie.eventscala.caseclasses.FrequencyRequirement

trait FrequencyStrategy {

  def onSubtreeCreated(context: ActorContext, nodeName: String, frequencyRequirement: FrequencyRequirement): Unit = ()

  def onEventEmit(context: ActorContext, nodeName: String, frequencyRequirement: FrequencyRequirement): Unit = ()

}
