package com.scalarookie.eventscalaTS.data

object Events {

  case object GraphCreated

  sealed trait Event
  case class Event1[A]                (e1: A)                                    extends Event
  case class Event2[A, B]             (e1: A, e2: B)                             extends Event
  case class Event3[A, B, C]          (e1: A, e2: B, e3: C)                      extends Event
  case class Event4[A, B, C, D]       (e1: A, e2: B, e3: C, e4: D)               extends Event
  case class Event5[A, B, C, D, E]    (e1: A, e2: B, e3: C, e4: D, e5: E)        extends Event
  case class Event6[A, B, C, D, E, F] (e1: A, e2: B, e3: C, e4: D, e5: E, e6: F) extends Event

}
