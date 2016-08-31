/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql.metrics

trait CQLMetrics {
  def time(name: String, time: Long): Unit
  def inc(name: String): Unit = inc(name, 1)
  def inc(name: String, delta: Long): Unit
  def dec(name: String): Unit = inc(name, -1)
  def dec(name: String, delta: Long) = inc(name, -delta): Unit
  def histogram(name: String, value: Long): Unit
}
