/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.internal

import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession => ScalaCouchbaseSession}
import akka.stream.javadsl.Source
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlQuery, Statement}

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{duration, Future}

/**
 * INTERNAL API
 */
@InternalApi
final class CouchbaseSessionJavaAdapter(delegate: ScalaCouchbaseSession) extends CouchbaseSession {

  override def scalaDelegate: ScalaCouchbaseSession = delegate

  override def underlying: AsyncBucket = delegate.underlying

  override def insert(document: JsonDocument): CompletionStage[JsonDocument] = delegate.insert(document).toJava

  override def insert(
      document: JsonDocument,
      writeSettings: CouchbaseWriteSettings
  ): CompletionStage[JsonDocument] = delegate.insert(document, writeSettings).toJava

  override def get(id: String): CompletionStage[Optional[JsonDocument]] =
    futureOptToJava(delegate.get(id))

  override def get(id: String, timeout: Duration): CompletionStage[Optional[JsonDocument]] =
    futureOptToJava(delegate.get(id, FiniteDuration.apply(timeout.toNanos, duration.NANOSECONDS)))

  override def upsert(document: JsonDocument): CompletionStage[JsonDocument] = delegate.upsert(document).toJava

  override def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): CompletionStage[JsonDocument] =
    delegate.upsert(document, writeSettings).toJava

  override def remove(id: String): CompletionStage[Done] = delegate.remove(id).toJava

  override def remove(id: String, writeSettings: CouchbaseWriteSettings): CompletionStage[Done] =
    delegate.remove(id, writeSettings).toJava

  override def streamedQuery(query: N1qlQuery): Source[JsonObject, _root_.akka.NotUsed] =
    delegate.streamedQuery(query).asJava

  override def streamedQuery(query: Statement): Source[JsonObject, _root_.akka.NotUsed] =
    delegate.streamedQuery(query).asJava

  override def singleResponseQuery(query: Statement): CompletionStage[Optional[JsonObject]] =
    futureOptToJava(delegate.singleResponseQuery(query))

  override def singleResponseQuery(query: N1qlQuery): CompletionStage[Optional[JsonObject]] =
    futureOptToJava(delegate.singleResponseQuery(query))

  override def counter(id: String, delta: Long, initial: Long): CompletionStage[Long] =
    delegate.counter(id, delta, initial).toJava

  override def counter(
      id: String,
      delta: Long,
      initial: Long,
      writeSettings: CouchbaseWriteSettings
  ): CompletionStage[Long] = delegate.counter(id, delta, initial, writeSettings).toJava

  override def close(): CompletionStage[Done] = delegate.close().toJava

  override def createIndex(indexName: String, ignoreIfExist: Boolean, fields: AnyRef*): CompletionStage[Boolean] =
    delegate.createIndex(indexName, ignoreIfExist, fields).toJava

  private def futureOptToJava[T](future: Future[Option[T]]): CompletionStage[Optional[T]] =
    future.map(_.asJava)(ExecutionContexts.sameThreadExecutionContext).toJava
}
