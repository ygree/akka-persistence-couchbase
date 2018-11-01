/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{ AsyncN1qlQueryResult, AsyncN1qlQueryRow }
import rx.functions.Func1
import rx.{ Observable, Subscriber }

import scala.concurrent.{ Future, Promise }
import scala.util.Try

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] object RxUtilities {
  val unfoldRows = new Func1[AsyncN1qlQueryResult, Observable[AsyncN1qlQueryRow]] {
    def call(t: AsyncN1qlQueryResult): Observable[AsyncN1qlQueryRow] =
      t.rows()
  }

  val unfoldDocument = new Func1[AsyncN1qlQueryRow, JsonObject] {
    def call(row: AsyncN1qlQueryRow): JsonObject =
      row.value()

  }

  val unfoldJsonObjects = new Func1[AsyncN1qlQueryResult, Observable[JsonObject]] {
    def call(t: AsyncN1qlQueryResult): Observable[JsonObject] =
      t.rows().map(unfoldDocument)
  }

  def singleObservableToFuture[T](o: Observable[T], id: Any): Future[T] = {
    val p = Promise[T]
    o.single()
      .subscribe(new Subscriber[T]() {
        override def onCompleted(): Unit = p.tryFailure(new RuntimeException(s"No document found for $id"))
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

}
