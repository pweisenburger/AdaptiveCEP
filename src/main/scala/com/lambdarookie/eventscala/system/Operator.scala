package com.lambdarookie.eventscala.system

trait Host

object NoHost extends Host

trait Operator {
  val host: Host
  val dependencies: Seq[Operator]
}
