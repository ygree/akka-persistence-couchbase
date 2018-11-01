/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.query.{ NoOffset, Offset, Sequence, TimeBasedUUID }
import com.couchbase.client.java.{ AsyncBucket, CouchbaseCluster }
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.spi.persistence.{ OffsetDao, OffsetStore }
import akka.persistence.couchbase._
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject

import scala.concurrent.{ ExecutionContext, Future }

private[lagom] object CouchbaseOffset {
  def offsetKey(eventProcessorId: String, tag: String): String = s"$eventProcessorId-$tag"

}

/**
 * Internal API
 *
 * TODO do we need to support timeuuid offset in the case couchbase is used as an offset store from cassandra?
 */
private[lagom] abstract class CouchbaseOffsetStore(
  system:  ActorSystem,
  config:  ReadSideConfig,
  session: AsyncBucket) extends OffsetStore {

  import system.dispatcher

  def prepare(eventProcessorId: String, tag: String): Future[CouchbaseOffsetDao] = {
    // FIXME use the right dispatcher
    val offset: Future[Option[JsonDocument]] = zeroOrOneObservableToFuture(session.get(CouchbaseOffset.offsetKey(eventProcessorId, tag)))
    offset.map {
      case None => new CouchbaseOffsetDao(session, eventProcessorId, tag, NoOffset, system.dispatcher)
      case Some(a: JsonDocument) =>
        val offset = a.content().getLong("sequenceOffset")
        new CouchbaseOffsetDao(session, eventProcessorId, tag, Sequence(offset), system.dispatcher)
    }
  }

}

/**
 * Internal API
 */
private[lagom] final class CouchbaseOffsetDao(session: AsyncBucket, eventProcessorId: String, tag: String, override val loadedOffset: Offset, ec: ExecutionContext) extends OffsetDao {

  override def saveOffset(offset: Offset): Future[Done] = {
    bindSaveOffset(offset).execute(session, ec)
  }

  def bindSaveOffset(offset: Offset): CouchbaseAction = {
    offset match {
      case NoOffset => (ab: AsyncBucket, ec: ExecutionContext) => Future.successful(Done)
      case seq: Sequence => (ab: AsyncBucket, ec: ExecutionContext) => {
        val id = CouchbaseOffset.offsetKey(eventProcessorId, tag)
        singleObservableToFuture(ab.upsert(JsonDocument.create(id, JsonObject.create().put("sequenceOffset", seq.value))))
          .map(_ => Done)(ec)
      }
      case uuid: TimeBasedUUID => ??? // not allowed for couchbase or is it?
    }
  }

}

