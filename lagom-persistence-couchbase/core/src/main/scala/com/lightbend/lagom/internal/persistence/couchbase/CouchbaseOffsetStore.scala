/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseAction
import com.lightbend.lagom.spi.persistence.{OffsetDao, OffsetStore}

import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
private[lagom] object CouchbaseOffsetStore {
  def offsetKey(eventProcessorId: String, tag: String): String = s"$eventProcessorId-$tag"

  val SequenceOffsetField = "sequenceOffset"
  val UuidOffsetField = "uuidOffset"

}

/**
 * INTERNAL API
 */
@InternalApi
private[lagom] abstract class CouchbaseOffsetStore(system: ActorSystem,
                                                   config: ReadSideConfig,
                                                   couchbase: CouchbaseSession)
    extends OffsetStore {

  import CouchbaseOffsetStore._
  import system.dispatcher

  def prepare(eventProcessorId: String, tag: String): Future[CouchbaseOffsetDao] = {
    val jsonOffset: Future[Option[JsonDocument]] =
      couchbase.get(CouchbaseOffsetStore.offsetKey(eventProcessorId, tag))

    jsonOffset.map {
      case None => new CouchbaseOffsetDao(couchbase, eventProcessorId, tag, NoOffset, system.dispatcher)
      case Some(a: JsonDocument) =>
        val offset =
          if (a.content().containsKey(SequenceOffsetField)) {
            Sequence(a.content().getLong(SequenceOffsetField))
          } else if (a.content().containsKey(UuidOffsetField)) {
            val uuid = UUID.fromString(a.content().getString(UuidOffsetField))
            require(uuid.version() == 1) // only time based uuids supported
            TimeBasedUUID(uuid)
          } else {
            throw new IllegalArgumentException("Offset entry does not contain any of the supported offset fields")
          }
        new CouchbaseOffsetDao(couchbase, eventProcessorId, tag, offset, system.dispatcher)
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[lagom] final class CouchbaseOffsetDao(couchbase: CouchbaseSession,
                                              eventProcessorId: String,
                                              tag: String,
                                              override val loadedOffset: Offset,
                                              ec: ExecutionContext)
    extends OffsetDao {

  override def saveOffset(offset: Offset): Future[Done] = bindSaveOffset(offset).execute(couchbase, ec)

  // FIXME write guarantees
  def bindSaveOffset(offset: Offset): CouchbaseAction =
    offset match {
      case NoOffset =>
        new CouchbaseAction {
          override def execute(ab: CouchbaseSession, ec: ExecutionContext): Future[Done] = Future.successful(Done)
        }
      case uuid: TimeBasedUUID =>
        new CouchbaseAction {
          override def execute(ab: CouchbaseSession, ec: ExecutionContext): Future[Done] = {
            val id = CouchbaseOffsetStore.offsetKey(eventProcessorId, tag)
            val json = JsonDocument.create(
              id,
              JsonObject.create().put(CouchbaseOffsetStore.UuidOffsetField, uuid.value.toString)
            )
            ab.upsert(json).map(_ => Done)(ec)
          }
        }
      case seq: Sequence =>
        new CouchbaseAction {
          override def execute(ab: CouchbaseSession, ec: ExecutionContext): Future[Done] = {
            val id = CouchbaseOffsetStore.offsetKey(eventProcessorId, tag)
            val json =
              JsonDocument.create(id, JsonObject.create().put(CouchbaseOffsetStore.SequenceOffsetField, seq.value))
            ab.upsert(json).map(_ => Done)(ec)
          }
        }
    }

}
