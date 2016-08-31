package com.protectwise.logging

import scala.concurrent.duration._

object MDCKeys {
  val requestIdKey = "request_id"
  val countKey = "count"
  val durationKey = "duration_ms"
  val lagMeanKey = "lag_mean_ms"
  val lagMinKey = "lag_min_ms"
  val lagMaxKey = "lag_max_ms"
}

trait MDCKeys {

  abstract class MDCKey[T](val key: String) {
    def apply(v: T): (String, T) = {
      if (v == null) null
      else key -> v
    }
  }

  case object RequestId extends MDCKey[String](MDCKeys.requestIdKey)
  case object Count      extends MDCKey[Long](MDCKeys.countKey)
  case object DurationMs extends MDCKey[Long](MDCKeys.durationKey) {
    def apply(v: Long, d: TimeUnit): (String, Long) = {
      apply(Duration(v, d).toMillis)
    }
    def apply(v: Duration): (String, Long) = {
      if (v == null) null
      else apply(v.toMillis)
    }
  }
  case object LagMean extends MDCKey[Float](MDCKeys.lagMeanKey) {
    def apply(v: Iterable[Long]): (String, Float) = {
      if (v == null || v.isEmpty) null
      else {
        val ts = System.currentTimeMillis()
        key -> v.map(vv => ts - vv).reduce(_ + _).toFloat / v.size
      }
    }
  }
  case object LagMin extends MDCKey[Long](MDCKeys.lagMinKey) {
    def apply(v: Iterable[Long]): (String, Long) = {
      if (v == null || v.isEmpty) null
      else {
        val ts = System.currentTimeMillis()
        key -> v.map(vv => ts - vv).max
      }
    }
  }
  case object LagMax extends MDCKey[Long](MDCKeys.lagMaxKey) {
    def apply(v: Iterable[Long]): (String, Long) = {
      if (v == null || v.isEmpty) null
      else {
        val ts = System.currentTimeMillis()
        key -> v.map(vv => ts - vv).max
      }
    }
  }

  implicit def longToMdcDuration(l: Long): (String, Long) = DurationMs(l)
  implicit def durationToMdcDuration(d: Duration): (String, Long) = DurationMs(d.toMillis)

}
