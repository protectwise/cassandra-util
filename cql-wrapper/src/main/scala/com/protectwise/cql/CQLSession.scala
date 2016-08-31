/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql

import java.util
import java.util.concurrent.Callable

import com.datastax.driver.core.Session.State
import com.datastax.driver.core._
import com.google.common.cache.Cache
import com.google.common.util.concurrent.ListenableFuture
import com.protectwise.cql.exceptions.StatementPrepareException
import com.protectwise.cql.metrics.CQLMetrics
import com.protectwise.cql.tracing.{CQLTracer, NullTracer}
import com.protectwise.logging.Logging
import com.protectwise.util.Configuration

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class notimplemented(message: String = "") extends scala.annotation.StaticAnnotation

case class CQLSession(
  session: Session,
  tracer: CQLTracer = NullTracer,
  metricsTrackers: Seq[CQLMetrics],
  config: Configuration,
  preparedStatements: Cache[String, PreparedStatement]
) extends Session with Logging {



  override def executeAsync(query: String, values: util.Map[String, AnyRef]): ResultSetFuture = executeAsync(new SimpleStatement(query, values))

  override def execute(query: String, values: util.Map[String, AnyRef]): ResultSet = execute(new SimpleStatement(query, values))

  override def initAsync(): ListenableFuture[Session] = session.initAsync()


  override def getLoggedKeyspace: String = session.getLoggedKeyspace

  override def init(): Session = session.init()

  override def execute(query: String): ResultSet = session.execute(query)

  override def execute(query: String, values: AnyRef*): ResultSet = session.execute(query, values)

  override def execute(statement: Statement): ResultSet = session.execute(statement)

  override def executeAsync(query: String): ResultSetFuture = session.executeAsync(query)

  override def executeAsync(query: String, values: AnyRef*): ResultSetFuture = session.executeAsync(query, values)

  override def executeAsync(statement: Statement): ResultSetFuture = session.executeAsync(statement)

  @notimplemented("This returns a Java future, use prepareFuture instead")
  override def prepareAsync(query: String): ListenableFuture[PreparedStatement] = ???

  @notimplemented("This returns a Java future, use prepareFuture instead")
  override def prepareAsync(statement: RegularStatement): ListenableFuture[PreparedStatement] = ???

  override def closeAsync(): CloseFuture = session.closeAsync()

  override def close(): Unit = session.close()

  override def isClosed: Boolean = session.isClosed

  override def getCluster: Cluster = session.getCluster

  override def getState: State = session.getState

  def maxUnloggedBatchStatements: Int = config.get[Int]("max_unlogged_batch_statements", Int.MaxValue)

  def maxCounterBatchStatements: Int = config.get[Int]("max_counter_batch_statements", Int.MaxValue)


  def prepareFuture(statement: String)(implicit ec: ExecutionContext): Future[PreparedStatement] = Future(prepare(statement))

  def prepareFuture(statement: RegularStatement)(implicit ec: ExecutionContext): Future[PreparedStatement] = Future(prepare(statement))

  override def prepare(statement: RegularStatement) = prepare(statement, statement.getQueryString)

  override def prepare(statement: String): PreparedStatement = {
    prepare(new SimpleStatement(statement), statement)
  }

  protected def prepare(statement: RegularStatement, alias: String): PreparedStatement = {
    preparedStatements.get(alias, new Callable[PreparedStatement] {
      override def call(): PreparedStatement = {
        metrics.inc("prepare_count")
        metrics.time("prepare") {
          try {
            blocking { session.prepare(statement) }
          } catch { case NonFatal(e) =>
            throw StatementPrepareException(s"Exception preparing statement: ${e.getMessage}.  Underlying statement: $statement", e)
          }
        }
      }
    })
  }

  object metrics {
    def time(name: String, time: Long): Unit = metricsTrackers.foreach(_.time(name, time))

    def time[A](name: String)(f: => A): A = {
      val st = System.currentTimeMillis()
      try f finally {
        val et = System.currentTimeMillis() - st
        metricsTrackers.foreach(_.time(name, et))
      }
    }

    def timeFuture[T](name: String)(f: Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val st = System.currentTimeMillis()
      f andThen { case _ =>
        val et = System.currentTimeMillis() - st
        metricsTrackers.foreach(_.time(name, et))
      }
    }

    def inc(name: String): Unit = metricsTrackers.foreach(_.inc(name))

    def inc(name: String, delta: Long): Unit = metricsTrackers.foreach(_.inc(name, delta))

    def dec(name: String): Unit = metricsTrackers.foreach(_.dec(name))

    def dec(name: String, delta: Long = 1): Unit = metricsTrackers.foreach(_.dec(name, delta))

    def histogram(name: String, value: Long): Unit = metricsTrackers.foreach(_.histogram(name, value))

    def hostMetricsPath(host: Host): String = {
      s"${host.getDatacenter}.${host.getRack}.${host.getAddress.getHostAddress.replace('.','_')}"
    }

    def statExec(r: Future[ResultSet])(implicit ec: ExecutionContext): Future[ResultSet] = {
      val start = System.currentTimeMillis()
      inc("query.outstanding")
      r andThen {
        case Success(res) =>
          dec("query.outstanding")
          val ecinfo = res.getExecutionInfo
          val host = ecinfo.getQueriedHost
          inc("query.success")
          inc(s"query.hosts.${hostMetricsPath(host)}.query_count")
          time(s"query.hosts.${hostMetricsPath(host)}.exec_ms", System.currentTimeMillis() - start)
          histogram(s"tried_host_count", ecinfo.getTriedHosts.size())
        case Failure(e) =>
          dec("query.outstanding")
          inc("query.failure")
          inc(s"query.failure_reason.${e.getClass.getSimpleName.toLowerCase}")
      }
    }

    def statStatement(statement: CQLStatement)(implicit session: CQLSession): Statement = {
      val s = statement.toStatement
      val c = statement.classification(s)
      inc(s"statement.$c.count")
      s
    }

  }
}
