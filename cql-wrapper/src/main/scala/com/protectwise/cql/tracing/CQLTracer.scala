/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql.tracing

import com.protectwise.cql.tracing.CQLTracer.CQLTraceFork

import scala.concurrent.{ExecutionContext, Future}

object CQLTracer {
  trait CQLTraceFork {
    def apply[R](f: => R): R
    def apply[R](f: Future[R])(implicit ec: ExecutionContext): Future[R]
  }
}
trait CQLTracer {
  def apply[D](data: D): CQLTraceFork
}
