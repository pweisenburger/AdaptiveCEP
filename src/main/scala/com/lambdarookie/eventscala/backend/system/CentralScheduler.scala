package com.lambdarookie.eventscala.backend.system

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.lambdarookie.eventscala.backend.system.traits.System
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration.FiniteDuration

/**
  * Created by monur.
  */
case class CentralScheduler(latencyInterval: Int, bandwidthInterval: Int, throughputInterval: Int) {
  def run(system: System, actorSystem: ActorSystem): Unit = {
    if (latencyInterval > 0) actorSystem.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(latencyInterval, TimeUnit.SECONDS),
      runnable = () => {
        system.hosts.now.foreach(_.measureNeighborLatencies())
      })
    if (bandwidthInterval > 0) actorSystem.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(bandwidthInterval, TimeUnit.SECONDS),
      runnable = () => {
        system.hosts.now.foreach(_.measureNeighborBandwidths())
      })
    if (throughputInterval > 0) actorSystem.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(throughputInterval, TimeUnit.SECONDS),
      runnable = () => {
        system.hosts.now.foreach(_.measureNeighborThroughputs())
      })
  }
}

