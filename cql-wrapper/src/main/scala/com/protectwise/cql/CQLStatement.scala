package com.protectwise.cql

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Calendar, Date, GregorianCalendar, TimeZone}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy
import com.protectwise.concurrent.RichListenableFuture
import com.protectwise.cql.CQLStatement.{Classification, RichBB, classifierReg}
import play.api.libs.iteratee.Enumerator

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object CQLStatement {

  def convert(value: Any): AnyRef = (value match {
    case Some(v) => convert(v)
    case None => null
    case v: Array[Byte @unchecked] =>
      val b = ByteBuffer.allocate(v.size)
      b.put(v)
      b.rewind()
      b
    case v: List[_] => v.map(convert).asJava
    case v: Set[_] => v.map(convert).asJava
    case v: Map[_, _] => v.map(v => convert(v._1) -> convert(v._2)).asJava
    case v => v
  }).asInstanceOf[AnyRef]

  implicit class RichBB(val b: ByteBuffer) extends AnyVal {
    def getBytes: Array[Byte] = {
      val bb = b.duplicate()
      val r = Array.ofDim[Byte](bb.remaining())
      bb.get(r)
      r
    }
    def hex: String = s"[${getBytes.map(b=>f"$b%02x").mkString("")}]"
  }
  protected var classifierReg = "(?i)^\\s*(CREATE|ALTER|INSERT|UPDATE|DELETE|SELECT(?=.*FROM))\\s+(?:(?:TABLE|INTO|.*FROM|KEYSPACE)\\s+)?(?:([a-zA-Z0-9_]+)\\s*\\.\\s*)?([a-zA-Z0-9_]+)".r()

  case class Classification(queryClass: String, keyspace: String, table: String) {
    override def toString = s"$keyspace.$table.$queryClass"
  }

}

case class CQLStatement(
    cqlParts: Seq[String],
    rawParams: Seq[AnyRef],
    namedParams: Map[Symbol, Seq[Int]],
    timestamp: CQLTimestamp = CQLNoTimestamp,
    asPrepared: Boolean = false
  ) {
  if (cqlParts.size-1 != rawParams.size) {
    throw new RuntimeException(s"cqlParts must have exactly one more element as rawParams in order for arguments to line up correctly\nParts: $cqlParts\nParams: $rawParams")
  }

  protected def preparedStatement(implicit session: CQLSession): PreparedStatement = session.prepare(cql)

  def withTimestamp(ts: CQLTimestamp): CQLStatement = this.copy(timestamp = ts)

  def withTimestamp(ts: Long): CQLStatement = this.copy(timestamp = CQLTimestamp(ts))

  def withTimestamp(ts: Option[CQLTimestamp]): CQLStatement = ts.map(withTimestamp) getOrElse this

  def ++(other: CQLStatement): CQLBatch = {
    CQLBatch(Seq(this, other))
  }

  def +(other: String) = {
    copy(cqlParts = cqlParts.dropRight(1) :+ (cqlParts.last + other))
  }

  def +(other: CQLStatement) = {
    // To maintain parameter alignment, cql"A $p1 B" + cql"C $p2 D" must be the same as cql"A $p1 BC $p2 D"
    // This is CQLStatement(Seq("A ", " B"), Seq(p1)) + CQLStatement(Seq("C ", " D"), Seq(p2))
    // The end result of the statement portion must take the tail of the first concatenated with the head of the second
    // replacing the tail of the first, and dropping the head of the second, then both lists concatenated together.
    // So the final result is CQLStatement(Seq("A ", " BC ", " D"), Seq(p1, p2))
    val newParts: Seq[String] = (cqlParts.take(cqlParts.size - 1) :+ (cqlParts.last + other.cqlParts.head)) ++ other.cqlParts.tail
    // The other statement named parameter positions need to have their position value adjusted by an offset equal to
    // the number of parameters in the primary statement
    val offset = rawParams.size
    val otherParamsOffset = other.namedParams.mapValues(_.map(_ + offset))
    val newBinds: Map[Symbol, Seq[Int]] = (namedParams.toSeq ++ otherParamsOffset.toSeq).groupBy(_._1)
      .mapValues(v => v.map(_._2).reduce(_ ++ _))
    CQLStatement(newParts, rawParams ++ other.rawParams, newBinds, timestamp)
  }

  def classification(implicit session: CQLSession): Classification = {
    classification(toStatement)
  }

  def classification(statement: Statement): Classification = {
    classifierReg.findFirstMatchIn(cql.replaceAllLiterally("\n", " ")) match {
      case Some(m) =>
        m.subgroups match {
          case List(queryClass, null, table) => Classification(queryClass.toLowerCase, statement.getKeyspace match { case null => "unknown"; case x => x.toLowerCase }, table.toLowerCase)
          case List(queryClass, keyspace, table) => Classification(queryClass.toLowerCase, keyspace.toLowerCase, table.toLowerCase)
          case _ => Classification("unknown", "unknown", "unknown")
        }
      case None => Classification("unknown", "unknown", "unknown")
    }
  }

  def execute(fetchSize: Int = 0)(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel = null, retryPolicy: RetryPolicy = null): Future[ResultSet] = {
    val statement = session.metrics.statStatement(this)
    if (fetchSize > 0) {
      statement.setFetchSize(fetchSize)
    }
    if (consistencyLevel != null) {
      statement.setConsistencyLevel(consistencyLevel)
    }
    if (retryPolicy != null) {
      statement.setRetryPolicy(retryPolicy)
    }
    timestamp(statement)

    val c = classification(statement)

    val st = System.currentTimeMillis()
    val r: Future[ResultSet] = session.tracer(this) {
      session.metrics.timeFuture("statement.exec_ms")(session.executeAsync(statement).toScala)
    }
    session.metrics.statExec(r)
  }

  /** Select Batching
    * All statements in the batch are executed one at a time, and you are given an Enumerator of the resultant Row
    * objects as a single stream
    * @param session
    * @param ec
    * @return
    */
  def enumerate(fetchSize: Int = 0)(implicit session: CQLSession, ec: ExecutionContext, consistencyLevel: ConsistencyLevel = null, retryPolicy: RetryPolicy = null): Enumerator[Row] = {
    Enumerator.flatten(execute(fetchSize).map { rs =>
      val ecinfo = rs.getExecutionInfo
      val host = ecinfo.getQueriedHost

      val it = rs.iterator().asScala map { row =>
        session.metrics.inc(s"query.hosts.${session.metrics.hostMetricsPath(host)}.row_count")
        row
      }
      Enumerator.enumerate(it)
    })
  }

  def prepare(): CQLStatement = {
    copy(asPrepared = true)
  }

  protected var _statement: Option[Statement] = None
  protected[cql] def toStatement(implicit session: CQLSession): Statement = {
    val r = _statement getOrElse {
      val (_, params) = cqlAndParams

      if (asPrepared) {
        // Helper to do some data type translation (prepared statements will complain about
        // data type mismatches even when types are compatible in raw CQL such as Date and Long
        timestamp(preparedStatement.bind(finalConvert(params): _*))
      } else {
        timestamp(new SimpleStatement(cql, params: _*))
      }
    }
    _statement = Some(r)
    r
  }

  /**
   * Provides final parameter type conversion, using data type hints from the prepared statement if possible
   * @return
   */
  protected def finalConvert(params: Seq[AnyRef])(implicit session: CQLSession): Seq[AnyRef] = {
    asPrepared match {
      case false => parameters
      case true =>
        preparedStatement.getVariables.asList().asScala zip params map { case (definition, param) =>
          param match {
            case null => null
            case p: java.lang.Long if definition.getType == DataType.timestamp() => new java.util.Date(p)
            case p: ByteBuffer if definition.getType == DataType.timestamp() =>
              new java.util.Date(p.duplicate().getLong)
            case p: ByteBuffer if definition.getType == DataType.text() =>
              new String(p.getBytes, "UTF-8")
            case p: ByteBuffer if definition.getType == DataType.inet() =>
              InetAddress.getByAddress(p.getBytes)
            case p: ByteBuffer if definition.getType == DataType.cint() =>
              p.duplicate().getInt
            case p: ByteBuffer if definition.getType == DataType.bigint() =>
              p.duplicate().getLong
            case p: ByteBuffer if definition.getType == DataType.counter() =>
              p.duplicate().getLong
            case p: ByteBuffer if definition.getType == DataType.cfloat() =>
              p.duplicate().getFloat
            case p: ByteBuffer if definition.getType == DataType.cdouble() =>
              p.duplicate().getDouble
            case t: Product if definition.getType.isInstanceOf[TupleType] =>
              definition.getType.asInstanceOf[TupleType].newValue(t.productIterator.toSeq.map(_.asInstanceOf[AnyRef]):_*)
            case _ =>
              param
          }
        } map(_.asInstanceOf[AnyRef])
    }
  }

  override def toString: String = cql

  def cql = cqlAndParams._1.mkString("?")

  protected lazy val cqlAndParams: (Seq[String], Seq[AnyRef]) = {
    // Initialize the cql buffer with the first entry in
    val cqlBuffer = mutable.Buffer[String](cqlParts.head)
    val partsBuffer = mutable.Buffer[AnyRef]()

    // cqlParts.tail.size and rawParams.size should be the same because cqlParts always has
    // one more element than rawParams due to how StringContext works.
    for ( (s, p) <- cqlParts.tail zip rawParams) {
      p match {
        case Inline(text) => // Also picks up NoArg
          // Overwrite the last entry with the concatenated form of these two
          cqlBuffer.append(cqlBuffer.remove(cqlBuffer.size-1) + text + s)
        case VarArgs(values, separator) =>
          // Create values.size - 1 separators, with a gap in the string between each.
          cqlBuffer.append(values.drop(1).map(_ => separator).toSeq:_*)
          cqlBuffer.append(s)
          partsBuffer.append(values.toSeq.map(CQLStatement.convert):_*)
        case KeyValArgs(keyVals, kvSep, pairSep) =>
          var first = true
          keyVals.map {
            case (k, v) if first =>
              first = false
              // The first one replaces & concatenates the last entry prior (similar to Inline())
              cqlBuffer.append(cqlBuffer.remove(cqlBuffer.size-1) + s"$k$kvSep")
            case (k, v) =>
              // Entry 2+ has separators and is otherwise normal.
              cqlBuffer.append(s"$pairSep$k$kvSep")
          }
          cqlBuffer.append(s)
          partsBuffer.append(keyVals.toSeq.map(_._2).map(CQLStatement.convert):_*)
        case _ =>
          // Normal case, add one to each buffer
          partsBuffer.append(p)
          cqlBuffer.append(s)
      }
    }
    (cqlBuffer.toSeq, partsBuffer.toSeq)
  }

  def debugString(implicit session: CQLSession): String = {
    val strings = cqlAndParams._1.iterator
    val parms = finalConvert(cqlAndParams._2).iterator

    val buf = new StringBuffer(strings.next())
    while (strings.hasNext) {
      buf append quote(parms.next())
      buf append strings.next()
    }
    buf.append(timestamp.toString)
    buf append ";"
    buf.toString
  }

  def parameters: Seq[AnyRef] = {
    cqlAndParams._2
  }

  protected def quote(part: Any): String = part match {
    case null => "null"
    case s: String => s"'${s.replace("'", "''")}'"
    case i: InetAddress => i.getHostAddress
    case b: ByteBuffer => quote(b.duplicate().rewind().array())
    case l: Array[Byte] => "0x" + l.map(v => f"$v%02x").mkString("")
    case l: java.util.List[_] => l.asScala.map(quote).mkString("[", ",", "]")
    case s: java.util.Set[_] => s.asScala.map(quote).mkString("{", ",", "}")
    case m: java.util.Map[_, _] => m.asScala.map(v => s"${quote(v._1)}:${quote(v._2)}").mkString("{", ",", "}")
    case d: java.util.Date => f"'${getIsoDate(d)}'"
    case o => s"$o"
  }

  override def equals(other: Any): Boolean = other match {
    case st: CQLStatement => toString equals other.toString
    case _ => false
  }

//  def withNamedValues(newValues: (Symbol, Any)*): CQLStatement = {
//    val positionedParams = newValues.flatMap { case (s: Symbol, v: Any) =>
//        assert(namedParams.contains(s), s"Named param ${s.name} is not defined in the statement")
//        namedParams(s).map(_ -> v)
//    }.sortBy(_._1).map(_._2)
//    assert(newValues.size == rawParams.size, s".withValues with wrong number of parameters (got ${newValues.size}, expected ${rawParams.size}")
//    copy(rawParams = positionedParams.map(CQLStatement.convert))
//  }


  def withValues(newValues: Any*): CQLStatement = {
    def isAnyVal[T](x: T)(implicit evidence: T <:< AnyVal = null) = evidence != null

    val namedP: Seq[(Int, Any)] = newValues.collect {
      case (s: Symbol, v: Any) =>
        assert(namedParams.contains(s), s"Named param ${s.name} is not defined in the statement")
        namedParams(s).map(_ -> v)
    }.flatten

    val posP: Seq[(Int, Any)] = newValues.zipWithIndex.collect {
      case (v: Any, idx) if isAnyVal(v) =>
        println(s"Got primitive $v")
        idx -> v
      case (v: AnyRef, idx) if !v.isInstanceOf[(Symbol, Any) @unchecked] || !v.asInstanceOf[(Symbol, Any)]._1.isInstanceOf[Symbol] =>
        idx -> v
    } map { case (idx,v) =>
      // If mixing named and positional parameters, the positional parameter index needs to be offset to get the right position
      (idx + namedP.count(_._1 <= idx)) -> v
    }

    val positionedParams = (namedP ++ posP).sortBy(_._1).map(_._2)

//    val positionedParams = newValues.zipWithIndex.flatMap {
//      case ((s: Symbol, v: Any), idx) =>
//        assert(namedParams.contains(s), s"Named param ${s.name} is not defined in the statement")
//        namedParams(s).map(_ -> v)
//      case (v: Any, idx) =>
//        Some(idx -> v)
//    }.sortBy(_._1).map(_._2)

    assert(newValues.size == rawParams.size, s".withValues with wrong number of parameters (got ${newValues.size}, expected ${rawParams.size}")
    copy(rawParams = positionedParams.map(CQLStatement.convert))
  }

  def stripMargin: CQLStatement = {
    copy(cqlParts = cqlParts.map(_.stripMargin))
  }

  def stripMargin(char: Char): CQLStatement = {
    copy(cqlParts = cqlParts.map(_.stripMargin(char)))
  }

  protected def getIsoDate(date: Date): String = {
    val d: Calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    d.setTime(date)
    f"${d.get(Calendar.YEAR)}-${d.get(Calendar.MONTH) + 1}%02d-${d.get(Calendar.DAY_OF_MONTH)}%02d" +
    f"T${d.get(Calendar.HOUR_OF_DAY)}%02d:${d.get(Calendar.MINUTE)}%02d:${d.get(Calendar.SECOND)}%02d.${d.get(Calendar.MILLISECOND)}%03dZ"
  }

}
