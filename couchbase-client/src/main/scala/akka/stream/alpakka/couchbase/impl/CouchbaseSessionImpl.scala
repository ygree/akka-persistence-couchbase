/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{ JsonDocument, JsonLongDocument }
import com.couchbase.client.java.query.{ N1qlQuery, Statement }
import rx.{ RxReactiveStreams, Subscriber }

import scala.concurrent.{ Future, Promise }

/**
 * InternalAPI
 */
@InternalApi
final private[couchbase] class CouchbaseSessionImpl(bucket: Bucket) extends CouchbaseSession {

  private val asyncBucket = bucket.async()
  import RxUtilities._

  def insert(document: JsonDocument): Future[JsonDocument] = {
    singleObservableToFuture(asyncBucket.insert(document), document)
  }

  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] = {
    singleObservableToFuture(asyncBucket.insert(document, writeSettings.persistTo, writeSettings.timeout, writeSettings.timeUnit), document)
  }

  def get(id: String): Future[Option[JsonDocument]] = {
    zeroOrOneObservableToFuture(asyncBucket.get(id))
  }

  def upsert(document: JsonDocument): Future[JsonDocument] = {
    singleObservableToFuture(asyncBucket.upsert(document), document.id)
  }

  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] = {
    singleObservableToFuture(asyncBucket.upsert(document, writeSettings.persistTo, writeSettings.replicateTo, writeSettings.timeout, writeSettings.timeUnit), document.id)
  }

  override def remove(id: String): Future[Done] = {
    singleObservableToFuture(asyncBucket.remove(id), id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)
  }

  override def remove(id: String, writeSettings: CouchbaseWriteSettings): Future[Done] = {
    singleObservableToFuture(asyncBucket.remove(id, writeSettings.persistTo, writeSettings.replicateTo, writeSettings.timeout, writeSettings.timeUnit), id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)
  }

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed] = {
    // FIXME change this so that it does paging if possible
    // FIXME verify back pressure works
    // FIXME verify cancellation works
    Source.fromPublisher(RxReactiveStreams.toPublisher(
      asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects)))
  }

  def streamedQuery(query: Statement): Source[JsonObject, NotUsed] = {
    Source.fromPublisher(RxReactiveStreams.toPublisher(
      asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects)))
  }

  def singleResponseQuery(query: Statement): Future[Option[JsonObject]] = {
    singleResponseQuery(N1qlQuery.simple(query))
  }
  def singleResponseQuery(query: N1qlQuery): Future[Option[JsonObject]] = {
    zeroOrOneObservableToFuture(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects))
  }

  def counter(id: String, delta: Long, initial: Long): Future[Long] = {
    singleObservableToFuture(asyncBucket.counter(id, delta, initial), id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)
  }

  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): Future[Long] = {
    singleObservableToFuture(asyncBucket.counter(id, delta, initial, writeSettings.persistTo, writeSettings.replicateTo, writeSettings.timeout, writeSettings.timeUnit), id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)
  }

  def close(): Future[Done] = {
    if (!asyncBucket.isClosed) {
      singleObservableToFuture(asyncBucket.close(), "close")
        .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)
    } else {
      Future.successful(Done)
    }
  }

  override def toString: String = s"CouchbaseSession(${bucket.name()})"
}

