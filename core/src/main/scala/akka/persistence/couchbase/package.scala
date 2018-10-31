/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence

import rx.functions.Func1
import rx.{ Observable, Subscriber }

import scala.concurrent.{ Future, Promise }
import scala.util.Try

package object couchbase {

  def singleObservableToFuture[T](o: Observable[T]): Future[T] = {
    val p = Promise[T]
    o.single()
      .subscribe(new Subscriber[T]() {
        override def onCompleted(): Unit = ()
        override def onError(e: Throwable): Unit = p.tryFailure(e)
        override def onNext(t: T): Unit = p.tryComplete(Try(t))
      })
    p.future
  }

  def zeroOrOneObservableToFuture[T](o: Observable[T]): Future[Option[T]] = {
    val p = Promise[Option[T]]
    o.subscribe(new Subscriber[T]() {
      override def onCompleted(): Unit = p.tryComplete(Try(None))
      override def onError(e: Throwable): Unit = p.tryFailure(e)
      override def onNext(t: T): Unit = p.tryComplete(Try(Some(t)))
    })
    p.future
  }

  // 2.11 inference :(
  def toFunc1[A, B](f: A => B): Func1[A, B] = new Func1[A, B] {
    override def call(a: A): B = f(a)
  }

}
