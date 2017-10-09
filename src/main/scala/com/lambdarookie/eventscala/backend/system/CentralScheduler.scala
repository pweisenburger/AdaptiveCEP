package com.lambdarookie.eventscala.backend.system

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.lambdarookie.eventscala.backend.system.traits.{HostImpl, System}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

/**
  * Created by monur.
  */
case class CentralScheduler(initialDelay: Int, latencyInterval: Int, bandwidthInterval: Int, throughputInterval: Int) {
  def run(system: System, actorSystem: ActorSystem): Unit = {
    if (latencyInterval > 0) actorSystem.scheduler.schedule(
      initialDelay = FiniteDuration(initialDelay, TimeUnit.SECONDS),
      interval = FiniteDuration(latencyInterval, TimeUnit.SECONDS),
      runnable = () => {
        system.hosts.now.foreach(_.asInstanceOf[HostImpl].measureNeighborLatencies())
      })
    if (bandwidthInterval > 0) actorSystem.scheduler.schedule(
      initialDelay = FiniteDuration(initialDelay, TimeUnit.SECONDS),
      interval = FiniteDuration(bandwidthInterval, TimeUnit.SECONDS),
      runnable = () => {
        system.hosts.now.foreach(_.asInstanceOf[HostImpl].measureNeighborBandwidths())
      })
    if (throughputInterval > 0) actorSystem.scheduler.schedule(
      initialDelay = FiniteDuration(initialDelay, TimeUnit.SECONDS),
      interval = FiniteDuration(throughputInterval, TimeUnit.SECONDS),
      runnable = () => {
        system.hosts.now.foreach(_.asInstanceOf[HostImpl].measureNeighborThroughputs())
      })
  }
}

