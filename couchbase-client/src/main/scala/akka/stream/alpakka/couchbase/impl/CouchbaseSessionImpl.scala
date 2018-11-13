/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import java.util.concurrent.TimeUnit

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.{AsyncBucket, Bucket, Cluster}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlQuery, Statement}
import rx.RxReactiveStreams

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 *
 * @param cluster if provided, it will be shut down when `close()` is called
 *
 * InternalAPI
 */
@InternalApi
final private[couchbase] class CouchbaseSessionImpl(bucket: Bucket, cluster: Option[Cluster]) extends CouchbaseSession {
  private val asyncBucket = bucket.async()
  import RxUtilities._

  override def underlying: AsyncBucket = asyncBucket

  def insert(document: JsonDocument): Future[JsonDocument] =
    singleObservableToFuture(asyncBucket.insert(document), document)

  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] =
    singleObservableToFuture(
      asyncBucket.insert(document, writeSettings.persistTo, writeSettings.timeout.toMillis, TimeUnit.MILLISECONDS),
      document
    )

  def get(id: String): Future[Option[JsonDocument]] =
    zeroOrOneObservableToFuture(asyncBucket.get(id))

  def get(id: String, timeout: FiniteDuration): Future[Option[JsonDocument]] =
    zeroOrOneObservableToFuture(asyncBucket.get(id, timeout.toMillis, TimeUnit.MILLISECONDS))

  def upsert(document: JsonDocument): Future[JsonDocument] =
    singleObservableToFuture(asyncBucket.upsert(document), document.id)

  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] =
    singleObservableToFuture(asyncBucket.upsert(document,
                                                writeSettings.persistTo,
                                                writeSettings.replicateTo,
                                                writeSettings.timeout.toMillis,
                                                TimeUnit.MILLISECONDS),
                             document.id)

  def remove(id: String): Future[Done] =
    singleObservableToFuture(asyncBucket.remove(id), id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)

  def remove(id: String, writeSettings: CouchbaseWriteSettings): Future[Done] =
    singleObservableToFuture(asyncBucket.remove(id,
                                                writeSettings.persistTo,
                                                writeSettings.replicateTo,
                                                writeSettings.timeout.toMillis,
                                                TimeUnit.MILLISECONDS),
                             id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed] =
    // FIXME change this so that it does paging if possible
    // FIXME verify back pressure works
    // FIXME verify cancellation works
    Source.fromPublisher(RxReactiveStreams.toPublisher(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects)))

  def streamedQuery(query: Statement): Source[JsonObject, NotUsed] =
    Source.fromPublisher(RxReactiveStreams.toPublisher(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects)))

  def singleResponseQuery(query: Statement): Future[Option[JsonObject]] =
    singleResponseQuery(N1qlQuery.simple(query))
  def singleResponseQuery(query: N1qlQuery): Future[Option[JsonObject]] =
    zeroOrOneObservableToFuture(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects))

  def counter(id: String, delta: Long, initial: Long): Future[Long] =
    singleObservableToFuture(asyncBucket.counter(id, delta, initial), id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)

  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): Future[Long] =
    singleObservableToFuture(asyncBucket.counter(id,
                                                 delta,
                                                 initial,
                                                 writeSettings.persistTo,
                                                 writeSettings.replicateTo,
                                                 writeSettings.timeout.toMillis,
                                                 TimeUnit.MILLISECONDS),
                             id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)

  def close(): Future[Done] =
    if (!asyncBucket.isClosed) {
      singleObservableToFuture(asyncBucket.close(), "close")
        .map { _ =>
          // FIXME blocking on global ec right now
          cluster match {
            case Some(cluster) => cluster.disconnect()
            case None =>
          }
        }(ExecutionContexts.global())
        .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)
    } else {
      Future.successful(Done)
    }

  override def toString: String = s"CouchbaseSession(${bucket.name()})"
}
