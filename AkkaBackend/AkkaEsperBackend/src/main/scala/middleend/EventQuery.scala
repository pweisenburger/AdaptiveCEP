package middleend

sealed trait EventQuery

case class PrimitiveQuery(name: String) extends EventQuery
case class SequenceQuery(qs: List[EventQuery]) extends EventQuery
case class AndQuery(q0: EventQuery, q1: EventQuery) extends EventQuery
case class OrQuery(q0: EventQuery, q1: EventQuery) extends EventQuery
