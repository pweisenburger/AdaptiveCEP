package com.lambdarookie.eventscala.backend

import org.scalatest.FunSuiteLike
import com.lambdarookie.eventscala.backend.qos.QoSUnits._

/**
  * Created by monur.
  */
class QoSUnitTests extends FunSuiteLike {
  test("QoSUnits - TimeSpan") {
    assert(10.ms < 10.micros)
    assert(10.ms < 10.sec)
    assert(10.micros < 10.sec)
    assert(10.ms < 11.ms)
    assert(10.micros < 11.micros)
    assert(10.sec < 11.sec)
    assert(10.micros > 10.ms)
    assert(10.sec > 10.ms)
    assert(10.sec > 10.micros)
    assert(11.ms > 10.ms)
    assert(11.micros > 10.micros)
    assert(11.sec > 10.sec)
    assert(10.ms <= 10.micros)
    assert(10.ms <= 10.sec)
    assert(10.micros <= 10.sec)
    assert(10.ms <= 11.ms)
    assert(10.micros <= 11.micros)
    assert(10.sec <= 11.sec)
    assert(10.ms <= 10.ms)
    assert(10.micros <= 10.micros)
    assert(10.sec <= 10.sec)
    assert(10.sec - 9.sec == 1.sec)
    assert(10.micros - 9.micros == 1.micros)
    assert(10.ms - 9.ms == 1.ms)
    assert(!(10.sec - 9.sec == 2.sec))
  }

  test("QoSUnits - Distance") {
    assert(10.m < 10.km)
    assert(10.m < 11.m)
    assert(10.km < 11.km)
    assert(10.km > 10.m)
    assert(11.m > 10.m)
    assert(11.km > 10.km)
    assert(10.m <= 10.km)
    assert(10.m <= 11.m)
    assert(10.km <= 11.km)
    assert(10.m <= 10.m)
    assert(10.km <= 10.km)
    assert(10.m - 9.m == 1.m)
    assert(10.km - 9.km == 1.km)
    assert(1001.m - 1.km == 1.m)
    assert(!(10.m - 9.m == 2.m))
  }

  test("QoSUnits - Bit rate") {
    assert(10.kbps < 10.mbps)
    assert(10.kbps < 10.gbps)
    assert(10.mbps < 10.gbps)
    assert(10.kbps < 11.kbps)
    assert(10.mbps < 11.mbps)
    assert(10.gbps < 11.gbps)
    assert(10.mbps > 10.kbps)
    assert(10.gbps > 10.kbps)
    assert(10.gbps > 10.mbps)
    assert(11.kbps > 10.kbps)
    assert(11.mbps > 10.mbps)
    assert(11.gbps > 10.gbps)
    assert(10.kbps <= 10.mbps)
    assert(10.kbps <= 10.gbps)
    assert(10.mbps <= 10.gbps)
    assert(10.kbps <= 11.kbps)
    assert(10.mbps <= 11.mbps)
    assert(10.gbps <= 11.gbps)
    assert(10.kbps <= 10.kbps)
    assert(10.mbps <= 10.mbps)
    assert(10.gbps <= 10.gbps)
    assert(10.gbps - 9.gbps == 1.gbps)
    assert(10.mbps - 9.mbps == 1.mbps)
    assert(10.kbps - 9.kbps == 1.kbps)
    assert(!(10.gbps - 9.gbps == 2.gbps))
    assert(1025.kbps - 1.mbps == 1.kbps)
  }
}
