/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql.tracing

import com.protectwise.cql.tracing.CQLTracer.CQLTraceFork

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure, Try}

class CallbackTracer(callback: CQLTrace[_, _] => Unit) extends CQLTracer {

  class CallbackTracerFork[D](data: D) extends CQLTraceFork {
    def apply[R](f: => R): R = {
      val startTime = System.currentTimeMillis()
      val result = Try(f)
      callback(CQLTrace(data, startTime, System.currentTimeMillis(), result))

      result match {
        case Success(r) => r
        case Failure(e) => throw e
      }
    }

    def apply[R](f: Future[R])(implicit ec: ExecutionContext): Future[R] = {
      val startTime = System.currentTimeMillis()
      f andThen { case result =>
        callback(CQLTrace(data, startTime, System.currentTimeMillis(), result))
      }
    }
  }

  def apply[D](data: D) = new CallbackTracerFork(data)

}
