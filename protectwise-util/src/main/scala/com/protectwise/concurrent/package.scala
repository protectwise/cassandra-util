package com.protectwise

import scala.concurrent.{ExecutionContext, Future, Promise}
import com.google.common.util.{concurrent => google}

package object concurrent {

  /**
    * Let us convert a Google ListenableFuture to a Scala Promise or Future
    *
    * @param lf Google listenable future
    * @tparam T Data type to return
    */
  // Cannot extend AnyVal because of the nested class at "new google.FutureCallback"
  implicit class RichListenableFuture[T](lf: google.ListenableFuture[T]) {
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

}