/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql.tracing

import com.protectwise.cql.tracing.CQLTracer.CQLTraceFork

import scala.concurrent.{ExecutionContext, Future}

object NullTracer extends CQLTracer {
  case object NullTracerFork extends CQLTraceFork {
    override def apply[R](f: => R): R = f

    override def apply[R](f: Future[R])(implicit ec: ExecutionContext): Future[R] = f
  }
  override def apply[D](statements: D): CQLTraceFork = NullTracerFork
}
