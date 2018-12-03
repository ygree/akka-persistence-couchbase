/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.UUID

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.couchbase.internal.CouchbaseSchema.Fields
import akka.persistence.couchbase.internal._
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.functions.AggregateFunctions._
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration.Duration

object CouchbaseReadJournal {
  final val Identifier = "couchbase-journal.read"
}

/**
 * INTERNAL API (all access should be through the persistence query APIs)
 */
@InternalApi class CouchbaseReadJournal(eas: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournal
    with AsyncCouchbaseSession
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery
    with CurrentPersistenceIdsQuery
    // TODO actually implement:    with PersistenceIdsQuery
    {

  private implicit val system = eas
  private val log = Logging(system, configPath)

  protected implicit val executionContext = system.dispatcher
  private val serialization: Serialization = SerializationExtension(system)

  /** INTERNAL API */
  @InternalApi private[akka] val settings: CouchbaseReadJournalSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.read$""", "")
    val sharedConfig = system.settings.config.getConfig(commonPath)

    CouchbaseReadJournalSettings(sharedConfig)
  }

  protected val asyncSession: Future[CouchbaseSession] = CouchbaseSession(settings.sessionSettings, settings.bucket)
  asyncSession.failed.foreach { ex =>
    log.error(ex, "Failed to connect to couchbase")
  }

  system.registerOnTermination {
    closeCouchbaseSession()
  }

  /* Both these queries flattens the doc.messages (which can contain batched writes)
   * into elements in the result and adds a field for the persistence id from
   * the surrounding document. Note that the UNNEST name (m) must match the name used
   * for the array value in the index or the index will not be used for these
   */
  private val eventsByTagQuery =
    s"""
      |SELECT a.persistence_id, m.* FROM ${settings.bucket} a UNNEST messages AS m
      |WHERE a.type = "${CouchbaseSchema.JournalEntryType}"
      |AND ARRAY_CONTAINS(m.tags, $$tag) = true
      |AND m.ordering > $$fromOffset AND m.ordering <= $$toOffset
      |ORDER BY m.ordering
      |limit $$limit
    """.stripMargin

  private val eventsByPersistenceId =
    s"""
      |SELECT a.persistence_id, m.* from ${settings.bucket} a UNNEST messages AS m
      |WHERE a.type = "${CouchbaseSchema.JournalEntryType}"
      |AND a.persistence_id = $$pid
      |AND m.sequence_nr  >= $$from
      |AND m.sequence_nr <= $$to
      |ORDER by m.sequence_nr
      |LIMIT $$limit
    """.stripMargin

  // IS NOT NULL is needed to hit the index
  private val pidsQuery =
    s"""
       |SELECT DISTINCT(persistence_id) FROM ${settings.bucket}
       |WHERE type = "${CouchbaseSchema.JournalEntryType}"
       |AND persistence_id IS NOT NULL
     """.stripMargin

  override def eventsByPersistenceId(persistenceId: String,
                                     fromSequenceNr: Long,
                                     toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    internalEventsByPersistenceId(live = true, persistenceId, fromSequenceNr, toSequenceNr)

  override def currentEventsByPersistenceId(persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    internalEventsByPersistenceId(live = false, persistenceId, fromSequenceNr, toSequenceNr)

  private def internalEventsByPersistenceId(live: Boolean,
                                            persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    sourceWithCouchbaseSession { session =>
      def params(from: Long) =
        JsonObject
          .create()
          .put("pid", persistenceId)
          .put("from", from)
          .put("to", toSequenceNr)
          .put("limit", settings.pageSize)

      // TODO do the deleted to query first and start from higher of that and fromSequenceNr
      val source =
        Source
          .fromGraph(
            new N1qlQueryStage[Long](
              live,
              settings.pageSize,
              N1qlQuery.parameterized(eventsByPersistenceId, params(fromSequenceNr)),
              session.underlying,
              fromSequenceNr, { from =>
                if (from <= toSequenceNr) Some(N1qlQuery.parameterized(eventsByPersistenceId, params(from)))
                else None
              },
              (_, row) => row.value().getLong(Fields.SequenceNr) + 1
            )
          )
          .mapMaterializedValue(_ => NotUsed)

      source.mapAsync(1) { row: AsyncN1qlQueryRow =>
        CouchbaseSchema
          .deserializeEvent(row.value(), serialization)
          .map(tpr => EventEnvelope(Offset.sequence(tpr.sequenceNr), tpr.persistenceId, tpr.sequenceNr, tpr.payload))
      }
    }

  /**
   * @param offset Either no offset or a timebased UUID offset, result will be exclusive the given UUID
   * @return A stream of tagged events sorted by their UUID
   */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    internalEventsByTag(live = true, tag, offset)

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    internalEventsByTag(live = false, tag, offset)

  private def internalEventsByTag(live: Boolean, tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    sourceWithCouchbaseSession { session =>
      val initialOrdering: UUID = offset match {
        case NoOffset => TimeBasedUUIDs.MinUUID
        case Sequence(o) =>
          throw new IllegalArgumentException("Couchbase Journal does not support sequence based offsets")
        case TimeBasedUUID(value) => value
      }
      val initialOrderingString = TimeBasedUUIDSerialization.toSortableString(initialOrdering)

      log.debug("events by tag: live {}, tag: {}, offset: {}", live, tag, initialOrderingString)

      def endOffset: String = {
        val uuid =
          if (settings.eventByTagSettings.eventualConsistencyDelay > Duration.Zero) {
            // offset delay ms back in time from now
            TimeBasedUUIDs.create(
              UUIDTimestamp.fromUnixTimestamp(
                System.currentTimeMillis() - (settings.eventByTagSettings.eventualConsistencyDelay.toMillis)
              ),
              TimeBasedUUIDs.MinLSB
            )
          } else TimeBasedUUIDs.MaxUUID

        TimeBasedUUIDSerialization.toSortableString(uuid)
      }

      def params(fromOffset: String, toOffset: String): JsonObject =
        JsonObject
          .create()
          .put("tag", tag)
          .put("ordering", fromOffset)
          .put("limit", settings.pageSize)
          .put("fromOffset", fromOffset)
          .put("toOffset", toOffset)

      // note that the result is "unnested" into the message objects + the persistence id, so the normal
      // document structure does not apply here
      val taggedRows: Source[AsyncN1qlQueryRow, NotUsed] =
        Source
          .fromGraph(
            new N1qlQueryStage[String](
              live,
              settings.pageSize,
              N1qlQuery.parameterized(eventsByTagQuery, params(initialOrderingString, endOffset)),
              session.underlying,
              initialOrderingString,
              ordering => Some(N1qlQuery.parameterized(eventsByTagQuery, params(ordering, endOffset))), { (_, row) =>
                row.value().getString(Fields.Ordering)
              }
            )
          )
          .mapMaterializedValue(_ => NotUsed)

      @volatile var lastUUID = TimeBasedUUIDs.MinUUID

      taggedRows.mapAsync(1) { row: AsyncN1qlQueryRow =>
        val value = row.value()
        CouchbaseSchema.deserializeTaggedEvent(value, Long.MaxValue, serialization).map { tpr =>
          if (TimeBasedUUIDComparator.comparator.compare(tpr.offset, lastUUID) < 0)
            throw new RuntimeException(
              s"Saw time based uuids go backward, last previous [$lastUUID], saw [${tpr.offset}]"
            )
          else
            lastUUID = tpr.offset
          EventEnvelope(Offset.timeBasedUUID(tpr.offset), tpr.pr.persistenceId, tpr.pr.sequenceNr, tpr.pr.payload)
        }
      }
    }

  /**
   * select  distinct persistenceId from akka where persistenceId is not null
   */
  override def currentPersistenceIds(): Source[String, NotUsed] = sourceWithCouchbaseSession { session =>
    // this type works on the current queries we'd need to create a stage
    // to do the live queries
    // this can fail as it relies on async updates to the index.
    val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS) // FIXME drop this consistency
    // FIXME respect page size for this query as well?
    session.streamedQuery(N1qlQuery.simple(pidsQuery, queryParams)).map(_.getString(Fields.PersistenceId))
  }

}
