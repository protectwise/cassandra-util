/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.datastax.driver.core.{DataType, ResultSet, Row}
import play.api.libs.iteratee.{Enumeratee, Enumerator}

import scala.collection.generic.CanBuildFrom
import scala.collection.{GenTraversableOnce, mutable}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect._
import scala.util.{Failure, Try}
import scala.collection.JavaConverters._
import com.google.common.util.{concurrent => google}

object Implicits {

  private implicit class RichListenableFuture[T](lf: google.ListenableFuture[T]) {
    def toPromise(implicit executor: ExecutionContext): Promise[T] = {
      val p = Promise[T]()
      google.Futures.addCallback(lf, new google.FutureCallback[T] {
        def onFailure(t: Throwable): Unit = p failure t

        def onSuccess(result: T): Unit = p success result
      })
      p
    }

    def toFuture(implicit executor: ExecutionContext): Future[T] = toPromise.future

    def toScala(implicit executor: ExecutionContext): Future[T] = toPromise.future

    def map[S](e: T => S)(implicit executor: ExecutionContext) = toPromise.future.map(e)

    def flatMap[S](e: T => Future[S])(implicit executor: ExecutionContext) = toPromise.future.flatMap(e)
  }


  implicit class RichResultSet(val rs: ResultSet) extends AnyVal {

    def toIterator: Iterator[Row] = rs.iterator().asScala

    def toIterable: Iterable[Row] = rs.iterator().asScala.toIterable

    /**
     * Map operation, but where we trigger a prefetch when the remaining available
     * fetched rows drops below a provided threshold.
     * @param fetchAtRemaining How many fetched but unprocessed rows should trigger fetching another page
     * @param f Transformation
     * @tparam R Result type of the transformation
     * @return
     */
    def mapPrefetch[R](fetchAtRemaining: Int)(f: Row=>R)(implicit ec: ExecutionContext): Iterable[R] = {
      var isFetching = false

      map { r =>
        if (!rs.isExhausted && rs.getAvailableWithoutFetching <= fetchAtRemaining && !isFetching) {
          isFetching = true
          rs.fetchMoreResults().toScala.onComplete(_ => isFetching = false)
        }
        f(r)
      }
    }

    def map[R](f: Row=>R): Iterable[R] = {
      toIterable.map(f)
    }

    /**
     * Flatmap operation, but where we trigger a prefetch when the remaining available fetched
     * rows drops below a provided threshold.
     * @param fetchAtRemaining How many fetched but unprocessed rows shoud trigger fetching another page
     * @param f Transformation
     * @return
     */
    def flatMapPrefetch[B, That](fetchAtRemaining: Int)(f: Row => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Iterable[Row], B, That], ec: ExecutionContext): That = {
      var isFetching = false

      flatMap { r =>
        if (!rs.isExhausted && rs.getAvailableWithoutFetching < fetchAtRemaining && !isFetching) {
          isFetching = true
          rs.fetchMoreResults().toScala.onComplete(_ => isFetching = false)
        }
        f(r)
      }
    }

    def flatMap[B, That](f: Row => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Iterable[Row], B, That]): That = {
      toIterable.flatMap(f)
    }

  }

  case class NullColumn(name: String) extends Throwable(s"$name is null", null, true, false) {
    override def fillInStackTrace(): Throwable = this
  }

  implicit class RichRow(val row: Row) extends AnyVal {

    def tryGet[T: ClassTag](col: String): Try[T] = Try { get(col) }

    def getOpt[T: ClassTag](col: String): Option[T] = row.tryGet(col).toOption

    def get[T: ClassTag](col: String, default: =>T): T =  tryGet(col) getOrElse default

    def get[T: ClassTag](col: String): T =
      if (row.isNull(col)) throw NullColumn(col)
      else {
        classTag[T].runtimeClass match {
          case t if t == classOf[Array[Byte]] =>
            val bb = row.getBytesUnsafe(col).duplicate()
            val b = Array.ofDim[Byte](bb.remaining())
            bb.get(b)
            b
          case t if t == classOf[ByteBuffer]  => row.getBytesUnsafe(col).duplicate()
          case t if t == classOf[Boolean]     => row.getBool(col)
          case t if t == classOf[InetAddress] => row.getInet(col)
          case t if t == classOf[Byte]        => row.getBytesUnsafe(col).duplicate().get()
          case t if t == classOf[Short]       => row.getBytesUnsafe(col).duplicate().getShort
          case t if t == classOf[Int]         => row.getInt(col)
          case t if t == classOf[Long] && row.getColumnDefinitions.getType(col).getName == DataType.date().getName =>
            // Let us retrieve Date (cql timestamp) as Long
            row.getTimestamp(col).getTime
          case t if t == classOf[Long]        => row.getLong(col)
          case t if t == classOf[Double]      => row.getDouble(col)
          case t if t == classOf[Float]       => row.getFloat(col)
          case t if t == classOf[UUID]        => row.getUUID(col)
          case t if t == classOf[String]      => row.getString(col)
          case t if t == classOf[Date]        => row.getTimestamp(col)
          case t if t == classOf[Null]        => null
          case t =>  throw new Exception(s"Type $t for column $col cannot be deserialized from a row")
        }
     }.asInstanceOf[T]


    def tryGetList[T: ClassTag](col: String): Try[List[T]] = Try(getList(col))

    def getListOpt[T: ClassTag](col: String): Option[List[T]] =
      if (row.isNull(col)) None
      else tryGetList(col).toOption

    def getList[T: ClassTag](col: String, default: => List[T]): List[T] = tryGetList(col) getOrElse default

    def getList[T: ClassTag](col: String): List[T] = if (row.isNull(col)) throw new NullColumn(col) else {
      classTag[T].runtimeClass match {
        case t if t == classOf[Array[Byte]] =>
          row.getList(col, classOf[ByteBuffer]).asScala.map { bb =>
            val b = Array.ofDim[Byte](bb.remaining())
            bb.duplicate().get(b)
            b
          }
        case t => row.getList(col, getDSClass(t)).asScala
      }
    }.map(_.asInstanceOf[T]).toList


    def tryGetSet[T: ClassTag](col: String): Try[Set[T]] = Try(getSet(col))

    def getSetOpt[T: ClassTag](col: String): Option[Set[T]] = tryGetSet(col).toOption

    def getSet[T: ClassTag](col: String, default: => Set[T]): Set[T] = tryGetSet(col) getOrElse default

    def getSet[T: ClassTag](col: String): Set[T] = if (row.isNull(col)) throw new NullColumn(col) else {
      classTag[T].runtimeClass match {
        case t if t == classOf[Array[Byte]] =>
          row.getSet(col, classOf[ByteBuffer]).asScala.map { bb =>
            val b = Array.ofDim[Byte](bb.remaining())
            bb.duplicate().get(b)
            b
          }
        case t => row.getSet(col, getDSClass(t)).asScala
      }
    }.map(_.asInstanceOf[T]).toSet


    def tryGetMap[K: ClassTag, V: ClassTag](col: String) = Try(getMap[K, V](col))

    def getMapOpt[K: ClassTag, V: ClassTag](col: String) = tryGetMap[K, V](col).toOption

    def getMap[K: ClassTag, V: ClassTag](col: String, default: => Map[K, V]): Map[K, V] = tryGetMap[K, V](col) getOrElse default

    def getMap[K: ClassTag, V: ClassTag](col: String): Map[K, V] = if (row.isNull(col)) throw new NullColumn(col) else {
      val rtK = classTag[K].runtimeClass match {
        case t if t == classOf[Array[Byte]] => classOf[ByteBuffer]
        case t => getDSClass(t)
      }
      val rtV = classTag[V].runtimeClass match {
        case t if t == classOf[Array[Byte]] => classOf[ByteBuffer]
        case t => getDSClass(t)
      }

      row.getMap(col, rtK, rtV).asScala map { case (k, v) =>
        ((k match {
          case kk: ByteBuffer if classTag[K].runtimeClass == classOf[Array[Byte]] =>
            val b = Array.ofDim[Byte](kk.remaining())
            kk.duplicate().get(b)
            b
          case kk => kk
        }).asInstanceOf[K], (v match {
          case vv: ByteBuffer if classTag[V].runtimeClass == classOf[Array[Byte]] =>
            val b = Array.ofDim[Byte](vv.remaining())
            vv.duplicate().get(b)
            b
          case vv => vv
        }).asInstanceOf[V])
      }
    }.toMap
  }

  protected def getDSClass(ct: Class[_]): Class[_] = {
    ct match {
      case t if t == classOf[Int]         => classOf[java.lang.Integer]
      case t if t == classOf[Long]        => classOf[java.lang.Long]
      case t if t == classOf[Double]      => classOf[java.lang.Double]
      case t if t == classOf[Byte]        => classOf[java.lang.Byte]
      case t if t == classOf[Boolean]     => classOf[java.lang.Boolean]
      case t if t == classOf[Short]       => classOf[java.lang.Short]
      case t if t == classOf[Float]       => classOf[java.lang.Float]
      // Expected handled fallthroughs, combined into one for performance
//      case t if t == classOf[ByteBuffer]  => t
//      case t if t == classOf[InetAddress] => t
//      case t if t == classOf[UUID]        => t
//      case t if t == classOf[String]      => t
//      case t if t == classOf[Date]        => t
//      case t if t == classOf[Null]        => t
      case t => t
    }
  }
}
