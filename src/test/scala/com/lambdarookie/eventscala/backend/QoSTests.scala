package com.lambdarookie.eventscala.backend

import com.lambdarookie.eventscala.backend.qos._
import org.scalatest.FunSuiteLike
import com.lambdarookie.eventscala.backend.data.QoSUnits._

/**
  * Created by monur.
  */
class QoSTests extends FunSuiteLike {
  test("QoS Demands - Latency") {
    val latency = Latency(null, null, 100.ms)
    val proximity1 = Proximity(null, null, 100.m)
    val proximity2 = Proximity(null, null, 200.m)
    val proximity3 = Proximity(null, null, 100.km)
    assert((latency lower 101.ms).isFulfilled)
    assert(!(latency lower 100.ms).isFulfilled)
    assert((latency lower 100.sec).isFulfilled)
    assert((latency lower 101.ms when(proximity1 within 100.m)).isFulfilled)
    assert(!(latency lower 100.ms when(proximity1 within 100.m)).isFulfilled)
    assert((latency lower 100.ms when(proximity2 within 100.m)).isFulfilled)
    assert((latency lower 100.ms when(proximity3 within 100.m)).isFulfilled)
  }

  test("QoS Conditions - Proximity") {
    val proximity1 = Proximity(null, null, 100.m)
    val proximity2 = Proximity(null, null, 200.m)
    val proximity3 = Proximity(null, null, 100.km)
    assert((proximity1 within 100.m).isFulfilled)
    assert(!(proximity1 within 99.m).isFulfilled)
    assert(!(proximity2 within 100.m).isFulfilled)
    assert(!(proximity3 within 100.m).isFulfilled)
  }
}
