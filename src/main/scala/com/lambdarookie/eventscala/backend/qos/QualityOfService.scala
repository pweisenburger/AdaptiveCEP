package com.lambdarookie.eventscala.backend.qos

import com.lambdarookie.eventscala.backend.system.Host

/**
  * Created by monur.
  */
trait QualityOfService {

}

case class Latency(host1: Host, host2: Host, latency: Long) extends QualityOfService {

}

case class Bandwidth(broker: Host, kbps: Long) {

}

