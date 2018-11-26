/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.annotation.{DoNotInherit, InternalApi}
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.internal.CouchbaseSessionJavaAdapter
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession => ScalaDslCouchbaseSession}
import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, CouchbaseWriteSettings}
import akka.stream.javadsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlQuery, Statement}
import com.couchbase.client.java.{AsyncBucket, Bucket}

import scala.compat.java8.FutureConverters._

object CouchbaseSession {

  /**
   * Create a session against the given bucket. The couchbase client used to connect will be created and then closed when
   * the session is closed.
   */
  def create(settings: CouchbaseSessionSettings, bucketName: String): CompletionStage[CouchbaseSession] =
    ScalaDslCouchbaseSession
      .apply(settings, bucketName)
      .map(new CouchbaseSessionJavaAdapter(_).asInstanceOf[CouchbaseSession])(
        ExecutionContexts.sameThreadExecutionContext
      )
      .toJava

  /**
   * Create a session against the given bucket. You are responsible for managing the lifecycle of the couchbase client
   * that the bucket was created with.
   */
  def create(bucket: Bucket): CouchbaseSession = new CouchbaseSessionJavaAdapter(ScalaDslCouchbaseSession.apply(bucket))
}

/**
 * Not for user extension
 *
 * abstract class, otherwise static forwarders are missing for companion object if building with Scala 2.11
 */
@DoNotInherit
abstract class CouchbaseSession {

  def underlying: AsyncBucket

  def asScala: ScalaDslCouchbaseSession

  /**
   * Insert a document using the default write settings
   *
   * @return A CompletionStage that completes with the written document when the write completes
   */
  def insert(document: JsonDocument): CompletionStage[JsonDocument]

  /**
   * Insert a document
   */
  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): CompletionStage[JsonDocument]

  /**
   * @return A document if found or none if there is no document for the id
   */
  def get(id: String): CompletionStage[Optional[JsonDocument]]

  /**
   * @param timeout fail the returned CompletionStage with a TimeoutException if it takes longer than this
   * @return A document if found or none if there is no document for the id
   */
  def get(id: String, timeout: Duration): CompletionStage[Optional[JsonDocument]]

  /**
   * Upsert using the default write settings
   *
   * @return a CompletionStage that completes when the upsert is done
   */
  def upsert(document: JsonDocument): CompletionStage[JsonDocument]

  /**
   * FIXME what happens if the id is missing?
   *
   * @return a CompletionStage that completes when the upsert is done
   */
  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): CompletionStage[JsonDocument]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return CompletionStage that completes when the document has been removed, if there is no such document
   *         the CompletionStage is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String): CompletionStage[Done]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return CompletionStage that completes when the document has been removed, if there is no such document
   *         the CompletionStage is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String, writeSettings: CouchbaseWriteSettings): CompletionStage[Done]

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed]
  def streamedQuery(query: Statement): Source[JsonObject, NotUsed]
  def singleResponseQuery(query: Statement): CompletionStage[Optional[JsonObject]]
  def singleResponseQuery(query: N1qlQuery): CompletionStage[Optional[JsonObject]]

  /**
   * Create or increment a counter
   *
   * @param id What counter document id
   * @param delta Value to increase the counter with if it does exist
   * @param initial Value to start from if the counter does not exist
   * @return The value of the counter after applying the delta
   */
  def counter(id: String, delta: Long, initial: Long): CompletionStage[Long]

  /**
   * Create or increment a counter
   *
   * @param id What counter document id
   * @param delta Value to increase the counter with if it does exist
   * @param initial Value to start from if the counter does not exist
   * @return The value of the counter after applying the delta
   */
  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): CompletionStage[Long]

  /**
   * Close the session and release all resources it holds. Subsequent calls to other methods will likely fail.
   */
  def close(): CompletionStage[Done]

  /**
   * Create a secondary index for the current bucket.
   *
   * @param indexName the name of the index.
   * @param ignoreIfExist if a secondary index already exists with that name, an exception will be thrown unless this
   *                      is set to true.
   * @param fields the JSON fields to index - each can be either `String` or [[com.couchbase.client.java.query.dsl.Expression]]
   * @return an [[java.util.concurrent.CompletionStage]] of true if the index was/will be effectively created, false
   *      if the index existed and ignoreIfExist is true. Completion of the CompletionStage does not guarantee the index
   *      is online and ready to be used.
   */
  def createIndex(indexName: String, ignoreIfExist: Boolean, fields: AnyRef*): CompletionStage[Boolean]
}
