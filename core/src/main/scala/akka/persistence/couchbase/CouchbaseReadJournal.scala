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

  /* This query flattens the doc.messages into elements in the result and adds a field for the persistence id from
   * the surrounding document
   */
  // FIXME will this really be ordered for the entire result rather than just inside documents?
  private val eventsByTagQuery =
    s"""
      |SELECT a.persistence_id, m.* FROM ${settings.bucket} a UNNEST messages AS m
      |WHERE ARRAY_CONTAINS(m.tags, $$tag) = true
      |AND m.ordering > $$ordering
      |ORDER BY m.ordering
      |limit $$limit
    """.stripMargin

  private val eventsByPersistenceId =
    s"""
      |select * from ${settings.bucket}
      |where persistence_id = $$pid
      |and sequence_from  >= $$from
      |and sequence_from <= $$to
      |order by sequence_from
      |limit $$limit
    """.stripMargin

  case class EventsByPersistenceIdState(from: Long, to: Long)

  // FIXME, filter out messages based on toSerquenceNr when they have been saved into a batch
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
          .put("to", toSequenceNr)
          .put("limit", settings.pageSize)
          .put("from", from)

      // TODO do the deleted to query first and start from higher of that and fromSequenceNr
      val source =
        Source
          .fromGraph(
            new N1qlQueryStage[EventsByPersistenceIdState](
              live,
              settings.pageSize,
              N1qlQuery.parameterized(eventsByPersistenceId, params(fromSequenceNr)),
              session.underlying,
              EventsByPersistenceIdState(fromSequenceNr, 0), { state =>
                if (state.to >= toSequenceNr)
                  None
                else
                  Some(N1qlQuery.parameterized(eventsByPersistenceId, params(state.from)))
              },
              (_, row) =>
                EventsByPersistenceIdState(row.value().getObject(settings.bucket).getLong(Fields.SequenceFrom) + 1,
                                           row.value().getObject(settings.bucket).getLong(Fields.SequenceTo))
            )
          )
          .mapMaterializedValue(_ => NotUsed)

      eventsByPersistenceIdSource(source)
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

      def params(fromOffset: String): JsonObject =
        JsonObject
          .create()
          .put("tag", tag)
          .put("ordering", fromOffset)
          .put("limit", settings.pageSize)

      // note that the result is "unnested" into the message objects + the persistence id, so the normal
      // document structure does not apply here
      val taggedRows: Source[AsyncN1qlQueryRow, NotUsed] =
        Source
          .fromGraph(
            new N1qlQueryStage[String](
              live,
              settings.pageSize,
              N1qlQuery.parameterized(eventsByTagQuery, params(initialOrderingString)),
              session.underlying,
              initialOrderingString,
              ordering => Some(N1qlQuery.parameterized(eventsByTagQuery, params(ordering))), { (_, row) =>
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

  private def eventsByPersistenceIdSource(in: Source[AsyncN1qlQueryRow, NotUsed]): Source[EventEnvelope, NotUsed] =
    in.mapAsync(1) { row: AsyncN1qlQueryRow =>
        val value = row.value().getObject(settings.bucket)
        CouchbaseSchema.deserializeEvents(value, Long.MaxValue, serialization, CouchbaseSchema.extractEvent).map {
          deserialized =>
            deserialized.map(
              tpr =>
                EventEnvelope(Offset
                                .sequence(tpr.sequenceNr), // FIXME, should this be +1, check inclusivity of offsets
                              tpr.persistenceId,
                              tpr.sequenceNr,
                              tpr.payload)
            )
        }
      }
      .mapConcat(identity)

  /**
   * select  distinct persistenceId from akka where persistenceId is not null
   */
  override def currentPersistenceIds(): Source[String, NotUsed] = sourceWithCouchbaseSession { session =>
    // this type works on the current queries we'd need to create a stage
    // to do the live queries
    // this can fail as it relies on async updates to the index.
    val query = select(distinct(Fields.PersistenceId)).from(settings.bucket).where(x(Fields.PersistenceId).isNotNull)
    val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)

    session.streamedQuery(N1qlQuery.simple(query, queryParams)).map(_.getString(Fields.PersistenceId))
  }

  /*
     select  distinct persistenceId from akka where persistenceId is not null

     Without the  is not null the pi2 index isn't used

    {
  "plan": {
    "#operator": "Sequence",
    "~children": [
      {
        "#operator": "IndexScan3",
        "covers": [
          "cover ((`akka`.`persistenceId`))",
          "cover ((`akka`.`sequence_from`))",
          "cover ((meta(`akka`).`id`))"
        ],
        "distinct": true,
        "index": "pi2",
        "index_id": "cef0943ad658063e",
        "index_projection": {
          "entry_keys": [
            0
          ]
        },
        "keyspace": "akka",
        "namespace": "default",
        "spans": [
          {
            "exact": true,
            "range": [
              {
                "inclusion": 0,
                "low": "null"
              }
            ]
          }
        ],
        "using": "gsi"
      },
      {
        "#operator": "Parallel",
        "~child": {
          "#operator": "Sequence",
          "~children": [
            {
              "#operator": "Filter",
              "condition": "(cover ((`akka`.`persistenceId`)) is not null)"
            },
            {
              "#operator": "InitialProject",
              "distinct": true,
              "result_terms": [
                {
                  "expr": "cover ((`akka`.`persistenceId`))"
                }
              ]
            },
            {
              "#operator": "Distinct"
            },
            {
              "#operator": "FinalProject"
            }
          ]
        }
      },
      {
        "#operator": "Distinct"
      }
    ]
  },
  "text": "select  distinct persistenceId from akka where persistenceId is not null"
}


   */
  // override def persistenceIds(): Source[String, NotUsed] =
  //    ???
}
