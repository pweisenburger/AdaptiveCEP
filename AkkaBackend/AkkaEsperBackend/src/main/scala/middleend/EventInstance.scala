package middleend

import scala.beans.BeanProperty

sealed trait EventInstance {
  val nodeId: String
}

case class PrimitiveInstance(i0: Any, @BeanProperty nodeId: String) extends EventInstance
case class SequenceInstance(is: List[EventInstance], @BeanProperty nodeId: String) extends EventInstance
case class AndInstance(i0: EventInstance, i1: EventInstance, @BeanProperty nodeId: String) extends EventInstance
case class OrInstance(i0: Option[EventInstance], i1: Option[EventInstance], @BeanProperty nodeId: String) extends EventInstance
