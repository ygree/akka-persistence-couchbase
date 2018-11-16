/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.couchbase.CouchbaseSchema.Fields
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

class CouchbaseReadJournal(eas: ExtendedActorSystem, config: Config, configPath: String)
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

  private val settings: CouchbaseReadJournalSettings = {
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

  val pageSize: Int = 100 // FIXME from config

  private val eventsByTagQuery =
    """select * FROM akka
      |WHERE ANY tag IN akka.all_tags SATISFIES tag = $tag END
      |AND ordering >= $ordering
      |ORDER BY ordering
      |limit $limit
    """.stripMargin

  private val eventsByPersistenceId =
    """
      |select * from akka
      |where persistence_id = $pid
      |and sequence_from  >= $from
      |and sequence_from <= $to
      |order by sequence_from
      |limit $limit
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
      val params: JsonObject = JsonObject
        .create()
        .put("pid", persistenceId)
        .put("to", toSequenceNr)
        .put("limit", pageSize)

      val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)
      // TODO do the deleted to query first and start from higher of that and fromSequenceNr
      val source =
        Source
          .fromGraph(
            new N1qlQueryStage[EventsByPersistenceIdState](
              live,
              pageSize,
              N1qlQuery.parameterized(eventsByPersistenceId, params.put("from", fromSequenceNr), queryParams),
              params,
              session.underlying,
              EventsByPersistenceIdState(fromSequenceNr, 0),
              state => {
                if (state.to >= toSequenceNr)
                  None
                else
                  Some(N1qlQuery.parameterized(eventsByPersistenceId, params.put("from", state.from), queryParams))
              },
              (_, row) =>
                EventsByPersistenceIdState(row.value().getObject(settings.bucket).getLong(Fields.SequenceFrom) + 1,
                                           row.value().getObject(settings.bucket).getLong(Fields.SequenceTo))
            )
          )
          .mapMaterializedValue(_ => NotUsed)

      eventsByPersistenceIdSource(source)
    }

  /*
  Messages persisted together with PersistAll have the same Offset

   CREATE INDEX `tags` ON `akka`((all (`all_tags`)),`ordering`)
   */

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    internalEventsByTag(live = true, tag, offset)

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    internalEventsByTag(live = false, tag, offset)

  private def internalEventsByTag(live: Boolean, tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    sourceWithCouchbaseSession { session =>
      log.debug("events by tag: live {}, tag: {}, offset: {}", live, tag, offset)
      val initialOrdering: Long = offset match {
        case NoOffset => 0L
        case Sequence(o) => o
        case TimeBasedUUID(_) =>
          throw new IllegalArgumentException("Couchbase Journal does not support Timeuuid offsets")
      }

      val params: JsonObject = JsonObject
        .create()
        .put("tag", tag)
        .put("limit", pageSize)
      val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)

      val sourceOfRows =
        Source
          .fromGraph(
            new N1qlQueryStage[Long](
              live,
              pageSize,
              N1qlQuery.parameterized(eventsByTagQuery, params.put(Fields.Ordering, initialOrdering), queryParams),
              params,
              session.underlying,
              initialOrdering,
              ordering =>
                Some(N1qlQuery.parameterized(eventsByTagQuery, params.put(Fields.Ordering, ordering), queryParams)),
              (_, row) => row.value().getObject(settings.bucket).getLong(Fields.Ordering) + 1
            )
          )
          .mapMaterializedValue(_ => NotUsed)

      eventsByTagSource(
        sourceOfRows,
        tag
      )
    }

  private def eventsByPersistenceIdSource(in: Source[AsyncN1qlQueryRow, NotUsed]): Source[EventEnvelope, NotUsed] =
    in.mapAsync(1) { row: AsyncN1qlQueryRow =>
        val value = row.value().getObject(settings.bucket)
        CouchbaseSchema.deserializeEvents(value, Long.MaxValue, serialization, CouchbaseSchema.extractTaggedEvent).map {
          deserialized =>
            deserialized.map(
              tpr =>
                EventEnvelope(Offset
                                .sequence(tpr.pr.sequenceNr), // FIXME, should this be +1, check inclusivity of offsets
                              tpr.pr.persistenceId,
                              tpr.pr.sequenceNr,
                              tpr.pr.payload)
            )
        }
      }
      .mapConcat(identity)

  private def eventsByTagSource(in: Source[AsyncN1qlQueryRow, NotUsed], tag: String): Source[EventEnvelope, NotUsed] =
    in.mapAsync(1) { row: AsyncN1qlQueryRow =>
        val value = row.value().getObject(settings.bucket)
        CouchbaseSchema.deserializeEvents(value, Long.MaxValue, serialization, CouchbaseSchema.extractTaggedEvent).map {
          deserialized =>
            val ordering = value.getLong(Fields.Ordering)
            deserialized
              .filter(_.tags.contains(tag))
              .map(
                tpr =>
                  EventEnvelope(Offset
                                  .sequence(ordering + 1), // set to the next one so resume doesn't get this event back
                                tpr.pr.persistenceId,
                                tpr.pr.sequenceNr,
                                tpr.pr.payload)
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
