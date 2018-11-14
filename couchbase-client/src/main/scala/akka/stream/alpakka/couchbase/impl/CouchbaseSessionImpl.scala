/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import java.util.concurrent.{Callable, TimeUnit}

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlQuery, Statement}
import com.couchbase.client.java.{AsyncBucket, CouchbaseAsyncCluster}
import rx._
import rx.functions.Func1

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 *
 * @param cluster if provided, it will be shut down when `close()` is called
 *
 * InternalAPI
 */
@InternalApi
final private[couchbase] class CouchbaseSessionImpl(asyncBucket: Observable[AsyncBucket],
                                                    cluster: Option[CouchbaseAsyncCluster])
    extends CouchbaseSession {

  import RxUtilities._

  override def underlying: Observable[AsyncBucket] = asyncBucket

  def insert(document: JsonDocument): Future[JsonDocument] =
    singleObservableToFuture(asyncBucket.flatMap(flatMapResult(_.insert(document))), document)

  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] =
    singleObservableToFuture(
      asyncBucket.flatMap(
        flatMapResult(
          _.insert(document, writeSettings.persistTo, writeSettings.timeout.toMillis, TimeUnit.MILLISECONDS)
        )
      ),
      document
    )

  def get(id: String): Future[Option[JsonDocument]] =
    zeroOrOneObservableToFuture(asyncBucket.flatMap(flatMapResult(_.get(id))))

  def get(id: String, timeout: FiniteDuration): Future[Option[JsonDocument]] =
    zeroOrOneObservableToFuture(asyncBucket.flatMap(flatMapResult(_.get(id, timeout.toMillis, TimeUnit.MILLISECONDS))))

  def upsert(document: JsonDocument): Future[JsonDocument] =
    singleObservableToFuture(asyncBucket.flatMap(flatMapResult(_.upsert(document))), document.id)

  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] =
    singleObservableToFuture(
      asyncBucket.flatMap(
        flatMapResult(
          _.upsert(document,
                   writeSettings.persistTo,
                   writeSettings.replicateTo,
                   writeSettings.timeout.toMillis,
                   TimeUnit.MILLISECONDS)
        )
      ),
      document.id
    )

  def remove(id: String): Future[Done] =
    singleObservableToFuture(asyncBucket.flatMap(flatMapResult(_.remove(id))), id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)

  def remove(id: String, writeSettings: CouchbaseWriteSettings): Future[Done] =
    singleObservableToFuture(
      asyncBucket.flatMap(
        flatMapResult(
          _.remove(id,
                   writeSettings.persistTo,
                   writeSettings.replicateTo,
                   writeSettings.timeout.toMillis,
                   TimeUnit.MILLISECONDS)
        )
      ),
      id
    ).map((_: JsonDocument) => Done)(ExecutionContexts.sameThreadExecutionContext)

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed] =
    // FIXME change this so that it does paging if possible
    // FIXME verify back pressure works
    // FIXME verify cancellation works
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        asyncBucket
          .flatMap(flatMapResult(_.query(query)))
          .flatMap(RxUtilities.unfoldJsonObjects)
      )
    )

  private def flatMapResult[R](fun: AsyncBucket => Observable[R]) =
    new Func1[AsyncBucket, Observable[R]]() {
      override def call(b: AsyncBucket): Observable[R] = fun(b)
    }

  private def mapResult[T, R](fun: T => R) =
    new Func1[T, R]() {
      override def call(b: T): R = fun(b)
    }

  def streamedQuery(query: Statement): Source[JsonObject, NotUsed] =
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        asyncBucket
          .flatMap(flatMapResult(_.query(query)))
          .flatMap(RxUtilities.unfoldJsonObjects)
      )
    )

  def singleResponseQuery(query: Statement): Future[Option[JsonObject]] =
    singleResponseQuery(N1qlQuery.simple(query))
  def singleResponseQuery(query: N1qlQuery): Future[Option[JsonObject]] =
    zeroOrOneObservableToFuture(
      asyncBucket.flatMap(flatMapResult(_.query(query))).flatMap(RxUtilities.unfoldJsonObjects)
    )

  def counter(id: String, delta: Long, initial: Long): Future[Long] =
    singleObservableToFuture(asyncBucket.flatMap(flatMapResult(_.counter(id, delta, initial))), id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)

  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): Future[Long] =
    singleObservableToFuture(
      asyncBucket.flatMap(
        mapResult(
          _.counter(id,
                    delta,
                    initial,
                    writeSettings.persistTo,
                    writeSettings.replicateTo,
                    writeSettings.timeout.toMillis,
                    TimeUnit.MILLISECONDS)
        )
      ),
      id
    ).map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)

  def close(): Future[Done] = {
    val result: Observable[Done] = asyncBucket.flatMap(flatMapResult[Done](b => {
      if (b.isClosed) {
        Observable.fromCallable(new Callable[Done]() {
          override def call() = Done
        })
      } else {
        b.close()
          .map[Done](mapResult[java.lang.Boolean, Done]((_: java.lang.Boolean) => {
            Done
          }))
      }
    }))
    singleObservableToFuture(result, "close")
  }

//  override def toString: String = s"CouchbaseSession(${bucket.name()})"
  override def toString: String = s"CouchbaseSession(...)"
}
