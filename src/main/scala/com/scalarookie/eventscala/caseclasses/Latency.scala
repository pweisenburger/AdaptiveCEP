package com.scalarookie.eventscala.caseclasses

import java.time.{Duration, Instant}

case class LatencyRequest(time: Instant)
case class LatencyResponse(requestTime: Instant)
case class PathLatency(duration: Duration)
