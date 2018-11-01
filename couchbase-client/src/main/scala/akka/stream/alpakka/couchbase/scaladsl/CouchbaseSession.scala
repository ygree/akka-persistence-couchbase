/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase.scaladsl

import akka.annotation.DoNotInherit
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.impl.CouchbaseSessionImpl
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{ JsonDocument, JsonLongDocument }
import com.couchbase.client.java.query._

import scala.concurrent.Future

object CouchbaseSession {

  def apply(bucket: Bucket): CouchbaseSession =
    new CouchbaseSessionImpl(bucket)

}

// FIXME this is quite different from the Alpakka PR, to provide what felt natural for the journal, should we also provide corresponding Source/Flow/Sink factories
// FIXME I dodged the type parameter for couchbase on purpose because I wanted to avoid hooking into their serialization infra and type inference mess
// is there a need for it, or an alternative with something like ByteString for blobs that you can then deserialize in any way
// you want?

/**
 * Not for user extension
 */
@DoNotInherit
trait CouchbaseSession {

  /**
   * Insert a document using the default write settings
   *
   * @return A future that completes with the written document when the write completes,
   */
  def insert(document: JsonDocument): Future[JsonDocument]
  /**
   * Insert a document
   */
  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument]

  def get(id: String): Future[Option[JsonDocument]]

  /**
   * Upsert using the default write settings
   * @return a future that completes when the upsert is done
   */
  def upsert(document: JsonDocument): Future[JsonDocument]
  /**
   * FIXME what happens if the id is missing?
   * @return a future that completes when the upsert is done
   */
  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument]

  /**
   * Remove a document by id using the default write settings.
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String): Future[Done]

  /**
   * Remove a document by id using the default write settings.
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String, writeSettings: CouchbaseWriteSettings): Future[Done]

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed]
  def streamedQuery(query: Statement): Source[JsonObject, NotUsed]
  def singleResponseQuery(query: Statement): Future[Option[JsonObject]]
  def singleResponseQuery(query: N1qlQuery): Future[Option[JsonObject]]

  /**
   * Create or increment a counter
   * @param id What counter document id
   * @param delta Value to increase the counter with if it does exist
   * @param initial Value to start from if the counter does not exist
   * @return
   */
  def counter(id: String, delta: Long, initial: Long): Future[Long]

  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): Future[Long]

  /**
   * Close the session and release all resources it holds. Subsequent calls to other methods will likely fail.
   */
  def close(): Future[Done]
}

