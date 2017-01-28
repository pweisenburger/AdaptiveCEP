package com.scalarookie.eventscalaTS.data

object Events {

  case object GraphCreated

  sealed trait Event
  case class Event1(e1: Any)                                              extends Event
  case class Event2(e1: Any, e2: Any)                                     extends Event
  case class Event3(e1: Any, e2: Any, e3: Any)                            extends Event
  case class Event4(e1: Any, e2: Any, e3: Any, e4: Any)                   extends Event
  case class Event5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any)          extends Event
  case class Event6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any) extends Event

}
