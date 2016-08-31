package com.protectwise.cql

import java.util.concurrent.Callable

import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy
import com.google.common.cache.{RemovalNotification, RemovalListener, CacheBuilder}
import com.protectwise.concurrent.RichListenableFuture
import play.api.libs.iteratee.{Enumeratee, Enumerator}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import scala.util.control.NonFatal

object CQLBatch {

  def apply(statements: Iterable[CQLStatement] = Seq()) = CQLUnloggedBatch(statements.toSeq, CQLNoTimestamp)

  def apply(statement: CQLStatement) = CQLSingleStatementBatch(statement, CQLNoTimestamp)

  def empty = CQLUnloggedBatch(Nil, CQLNoTimestamp)
}

sealed trait CQLBatch {
  val statements: Seq[CQLStatement]
  val timestamp: CQLTimestamp

  def addStatement(statement: CQLStatement): CQLBatch

  def ++(statement: CQLStatement): CQLBatch

  def addStatements(statement: CQLStatement*): CQLBatch

  def ++(batch: CQLBatch): CQLBatch

  def execute()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]]

  @deprecated("use executeTokenAware", "bikeshed")
  def tokenAwareExecute()(implicit session: CQLSession, ec: ExecutionContext): Future[Iterable[ResultSet]] = executeTokenAware()(session, ec)

  def executeTokenAware()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]]

  def logged: CQLLoggedBatch = CQLLoggedBatch(statements, timestamp)

  def counter: CQLCounterBatch = CQLCounterBatch(statements, timestamp)

  def unlogged: CQLUnloggedBatch = CQLUnloggedBatch(statements, timestamp)

  def withTimestamp(ts: Long): CQLBatch = withTimestamp(CQLTimestamp(ts))

  def withTimestamp(ts: CQLTimestamp): CQLBatch

  def prepareAll(): CQLBatch

  def groupByFirstReplica(implicit session: CQLSession): Map[Host, CQLBatch] = {
    val meta = session.getCluster.getMetadata
    val config = session.getCluster.getConfiguration
    statements.groupBy { s =>
      val st = s.toStatement
      try {
        meta.getReplicas(st.getKeyspace, st.getRoutingKey(config.getProtocolOptions.getProtocolVersion, config.getCodecRegistry)).iterator().next
      } catch { case NonFatal(e) =>
        null
      }
    } mapValues { st => CQLBatch(st) }
  }

  /** Select Batching
    * All statements in the batch are executed one at a time, and you are given an Enumerator of the resultant Row
    * objects as a single stream
    * @param session
    * @param ec
    * @return
    */
  @deprecated("use enumerate()", "2015-02-06")
  def enumerateAllRows(implicit session: CQLSession, ec: ExecutionContext): Enumerator[Row] = enumerate()

  /** Select Batching
    * All statements in the batch are executed one at a time, and you are given an Enumerator of the resultant Row
    * objects as a single stream
    * @param session
    * @param ec
    * @return
    */
  def enumerate(fetchSize: Int = 0)(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel = null, retryPolicy: RetryPolicy = null): Enumerator[Row] = {
    Enumerator.enumerate(statements) through
    Enumeratee.mapFlatten(_.enumerate(fetchSize))
  }

  /**
   * Select batching with token aware concurrency {@see enumerate}.
   * This method concurrently executes reads across all replicas containing data in the batch
   * @param session
   * @param ec
   * @return
   */
  @deprecated("use enumerateConcurrently()", "2015-02-06")
  def enumerateAllRowsConcurrently(implicit session: CQLSession, ec: ExecutionContext): Enumerator[Row] = enumerateConcurrently()

  /**
   * Select batching with token aware concurrency {@see enumerate}.
   * This method concurrently executes reads across all replicas containing data in the batch
   * @param session
   * @param ec
   * @return
   */
  def enumerateConcurrently(fetchSize: Int = 0)(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel = null, retryPolicy: RetryPolicy = null): Enumerator[Row] = {
    val batches: Iterable[CQLBatch] = groupByFirstReplica(session).values
    batches.size match {
      case 0 =>
        // Don't try to bother Cassandra with something that can't be done.
        Enumerator.empty
      case 1 =>
        // The reduce stage would error with an EmptyReduceLeft exception
        enumerate(fetchSize)
      case _ =>
        // Normal operation mode, scatter to all the replicas and return results as they arrive.
        batches.map(_.enumerate(fetchSize)).reduce(_ interleave _)
    }
  }
}

case class CQLSingleStatementBatch(statement: CQLStatement, timestamp: CQLTimestamp = CQLNoTimestamp) extends CQLBatch {
  override val statements: Seq[CQLStatement] = Seq(statement)

  override def addStatements(statements: CQLStatement*): CQLBatch = CQLUnloggedBatch(statement +: statements, timestamp)

  override def execute()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    session.metrics.histogram(s"batch_size", 1)
    statement.execute().map(Seq(_))
  }

  @deprecated("use executeTokenAware", "bikeshed")
  override def tokenAwareExecute()(implicit session: CQLSession, ec: ExecutionContext): Future[Iterable[ResultSet]] = executeTokenAware()(session, ec)

  override def executeTokenAware()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] =
    execute()

  override def addStatement(statement: CQLStatement): CQLBatch = addStatements(Seq(statement):_*)

  override def ++(statement: CQLStatement): CQLBatch = addStatement(statement)

  override def ++(batch: CQLBatch): CQLBatch = addStatements(batch.statements:_*)

  def +(other: CQLSingleStatementBatch): CQLSingleStatementBatch = {
    copy(statement = statement + other.statement)
  }

  override def withTimestamp(ts: Long): CQLSingleStatementBatch = withTimestamp(CQLTimestamp(ts))

  override def withTimestamp(ts: CQLTimestamp): CQLSingleStatementBatch = copy(timestamp = ts)

  override def prepareAll() = copy(statement = statement.prepare())

  override def equals(other: Any): Boolean = other match {
    case st: CQLSingleStatementBatch => statement equals st.statement
    case _ => false
  }

  def withValues(values: Any*): CQLSingleStatementBatch = {
    copy(statement = statement.withValues(values:_*))
  }

  /** Select Batching
    * All statements in the batch are executed one at a time, and you are given an Enumerator of the resultant Row
    * objects as a single stream
    * @param session
    * @param ec
    * @return
    */
  override def enumerate(fetchSize: Int = 0)(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel = null, retryPolicy: RetryPolicy = null): Enumerator[Row] =
    statement.enumerate(fetchSize)
}

case class CQLUnloggedBatch(statements: Seq[CQLStatement], timestamp: CQLTimestamp = CQLNoTimestamp) extends CQLBatch {

  def addStatement(statement: CQLStatement): CQLUnloggedBatch = {
    CQLUnloggedBatch(statements :+ statement, timestamp)
  }

  def ++(statement: CQLStatement): CQLUnloggedBatch = addStatement(statement)

  def addStatements(statement: CQLStatement*): CQLUnloggedBatch = {
    CQLUnloggedBatch(statements ++ statement, timestamp)
  }

  def ++(batch: CQLBatch): CQLUnloggedBatch = addStatements(batch.statements:_*)

  def execute()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    Future.sequence(statements.grouped(session.maxUnloggedBatchStatements).toList.map {
      case List(statement) => statement.execute()
      case subStatements =>
        session.metrics.histogram(s"batch.all.size", subStatements.size)
        session.metrics.histogram(s"batch.unlogged.size", subStatements.size)

        val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
        timestamp(batch)

        subStatements.foreach { statement =>
          val st = session.metrics.statStatement(statement)
          if (consistencyLevel != null) st.setConsistencyLevel(consistencyLevel)
          if (retryPolicy != null) st.setRetryPolicy(retryPolicy)
          batch.add(st)
        }
        if (consistencyLevel != null) batch.setConsistencyLevel(consistencyLevel)
        if (retryPolicy != null) batch.setRetryPolicy(retryPolicy)

        val r = session.tracer(this)(
          session.metrics.timeFuture("batch.unlogged.exec_ms")(
            session.metrics.timeFuture("batch.all.exec_ms")(
              session.executeAsync(batch).toScala
            )
          )
        )
        session.metrics.statExec(r)
    })
  }

  @deprecated("use executeTokenAware", "bikeshed")
  override def tokenAwareExecute()(implicit session: CQLSession, ec: ExecutionContext): Future[Iterable[ResultSet]] = executeTokenAware()(session, ec)

  override def executeTokenAware()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    Future.sequence(groupByFirstReplica.values.map(_.execute())).map(_.flatten)
  }

  override def withTimestamp(ts: Long): CQLUnloggedBatch = withTimestamp(CQLTimestamp(ts))

  override def withTimestamp(ts: CQLTimestamp): CQLUnloggedBatch = copy(timestamp = ts)

  override def prepareAll() = copy(statements = statements.map(_.prepare()))
}

case class CQLLoggedBatch(statements: Seq[CQLStatement], timestamp: CQLTimestamp = CQLNoTimestamp) extends CQLBatch {
  override def addStatement(statement: CQLStatement): CQLLoggedBatch = copy(statements = statements :+ statement)

  def ++(statement: CQLStatement): CQLLoggedBatch = addStatement(statement)

  override def addStatements(statement: CQLStatement*): CQLLoggedBatch = copy(statements = statements ++ statement)

  def ++(batch: CQLBatch): CQLLoggedBatch = addStatements(batch.statements:_*)

  override def execute()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    session.metrics.histogram(s"batch.all.size", statements.size)
    session.metrics.histogram(s"batch.logged.size", statements.size)

    val batch = new BatchStatement(BatchStatement.Type.LOGGED)
    timestamp(batch)

    statements.foreach { statement =>
      val st = session.metrics.statStatement(statement)
      if (consistencyLevel != null) st.setConsistencyLevel(consistencyLevel)
      if (retryPolicy != null) st.setRetryPolicy(retryPolicy)
      batch.add(st)
    }
    if (consistencyLevel != null) batch.setConsistencyLevel(consistencyLevel)
    if (retryPolicy != null) batch.setRetryPolicy(retryPolicy)

    val r = session.tracer(this)(
      session.metrics.timeFuture("batch.logged.exec_ms")(
        session.metrics.timeFuture("batch.all.exec_ms")(
          session.executeAsync(batch).toScala
        )
      )
    )
    session.metrics.statExec(r).map(Seq(_))
  }

  @deprecated("use executeTokenAware", "bikeshed")
  override def tokenAwareExecute()(implicit session: CQLSession, ec: ExecutionContext): Future[Iterable[ResultSet]] = executeTokenAware()(session, ec)

  override def executeTokenAware()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    Future.sequence(groupByFirstReplica.values.map(_.execute())).map(_.flatten)
  }

  override def withTimestamp(ts: Long): CQLLoggedBatch = withTimestamp(CQLTimestamp(ts))

  override def withTimestamp(ts: CQLTimestamp): CQLLoggedBatch = copy(timestamp = ts)

  override def prepareAll() = copy(statements = statements.map(_.prepare()))

}

case class CQLCounterBatch(statements: Seq[CQLStatement], timestamp: CQLTimestamp = CQLNoTimestamp) extends CQLBatch {
  override def addStatement(statement: CQLStatement): CQLCounterBatch = copy(statements = statements :+ statement)

  def ++(statement: CQLStatement): CQLCounterBatch = addStatement(statement)

  override def addStatements(statement: CQLStatement*): CQLCounterBatch = copy(statements = statements ++ statement)

  def ++(batch: CQLBatch): CQLCounterBatch = addStatements(batch.statements:_*)

  override def execute()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    Future.sequence(statements.grouped(session.maxCounterBatchStatements).toList.map {
      case List(statement) => statement.execute()
      case subStatements =>
        session.metrics.histogram(s"batch.all.size", subStatements.size)
        session.metrics.histogram(s"batch.counter.size", subStatements.size)

        val batch = new BatchStatement(BatchStatement.Type.COUNTER)
        timestamp(batch)

        subStatements.foreach { statement =>
          val st = session.metrics.statStatement(statement)
          if (consistencyLevel != null) st.setConsistencyLevel(consistencyLevel)
          if (retryPolicy != null) st.setRetryPolicy(retryPolicy)
          batch.add(st)
        }
        if (consistencyLevel != null) batch.setConsistencyLevel(consistencyLevel)
        if (retryPolicy != null) batch.setRetryPolicy(retryPolicy)

        val r = session.tracer(this)(
          session.metrics.timeFuture("batch.counter.exec_ms")(
            session.metrics.timeFuture("batch.all.exec_ms")(
              session.executeAsync(batch).toScala
            )
          )
        )
        session.metrics.statExec(r)
    })
  }

  @deprecated("use executeTokenAware", "bikeshed")
  override def tokenAwareExecute()(implicit session: CQLSession, ec: ExecutionContext): Future[Iterable[ResultSet]] = executeTokenAware()(session, ec)

  override def executeTokenAware()(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel=null, retryPolicy: RetryPolicy=null): Future[Iterable[ResultSet]] = {
    Future.sequence(groupByFirstReplica.values.map(_.counter.execute())).map(_.flatten)
  }

  override def withTimestamp(ts: Long): CQLCounterBatch = withTimestamp(CQLTimestamp(ts))

  override def withTimestamp(ts: CQLTimestamp): CQLCounterBatch = copy(timestamp = ts)

  override def prepareAll() = copy(statements = statements.map(_.prepare()))

}
