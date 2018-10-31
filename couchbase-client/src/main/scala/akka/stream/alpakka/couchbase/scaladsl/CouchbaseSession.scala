/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase.scaladsl

import java.util
import java.util.Observer

import akka.{ Done, NotUsed }
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.impl.{ N1qlQueryStage, RxUtilities }
import akka.stream.scaladsl.Source
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.{ JsonDocument, JsonLongDocument }
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query._
import rx.functions.Func1
import rx.{ Observable, RxReactiveStreams, Subscriber }

import scala.concurrent.{ Future, Promise }
import scala.util.Try

object CouchbaseSession {
  /**
   * INTERNAL API
   */
  @InternalApi
  private[couchbase] def singleObservableToFuture[T](o: Observable[T], id: String): Future[T] = {
    val p = Promise[T]
    o.single()
      .subscribe(new Subscriber[T]() {
        override def onCompleted(): Unit = p.tryFailure(new RuntimeException(s"No document found for $id"))
        override def onError(e: Throwable): Unit = p.tryFailure(e)
        override def onNext(t: T): Unit = p.tryComplete(Try(t))
      })
    p.future
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[couchbase] def zeroOrOneObservableToFuture[T](o: Observable[T]): Future[Option[T]] = {
    val p = Promise[Option[T]]
    o.subscribe(new Subscriber[T]() {
      override def onCompleted(): Unit = p.tryComplete(Try(None))
      override def onError(e: Throwable): Unit = p.tryFailure(e)
      override def onNext(t: T): Unit = p.tryComplete(Try(Some(t)))
    })
    p.future
  }

  def apply(bucket: Bucket): CouchbaseSession =
    new CouchbaseSession(bucket)

}

final class CouchbaseSession(bucket: Bucket) {

  private val asyncBucket = bucket.async()
  import CouchbaseSession._

  def upsert(document: JsonDocument): Future[JsonDocument] = {
    singleObservableToFuture(asyncBucket.upsert(document), document.id)
  }

  /**
   * FIXME what happens if the id is missing?
   * @return a future that completes when the upsert is done
   */
  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[Done] = {
    singleObservableToFuture(asyncBucket.upsert(document, writeSettings.persistTo, writeSettings.replicateTo, writeSettings.timeout, writeSettings.timeUnit), document.id())
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)
  }

  /**
   * Fetch the document for a specific id, fails the future if there is no document with the given id
   */
  def get(id: String): Future[Option[JsonDocument]] = {
    zeroOrOneObservableToFuture(asyncBucket.get(id))
  }

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed] = {
    // FIXME change this so that it does paging if possible
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

  def insert(document: JsonDocument): Future[JsonDocument] = {
    val done = Promise[JsonDocument]()
    asyncBucket.insert(document).asObservable().subscribe(new Subscriber[JsonDocument]() {
      def onCompleted(): Unit = done.tryFailure(new IllegalStateException("Got no document back for insert"))
      def onError(e: Throwable): Unit = done.tryFailure(e)
      def onNext(t: JsonDocument): Unit = done.trySuccess(t)
    })
    done.future
  }

  def counter(id: String, delta: Long, initial: Long): Future[JsonLongDocument] = {
    singleObservableToFuture(asyncBucket.counter(id, delta, initial), id)
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
