/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.UUID

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.couchbase.internal.CouchbaseSchema.{Fields, Queries}
import akka.persistence.couchbase.internal._
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry
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
import scala.concurrent.duration._

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
    with PersistenceIdsQuery
    with CouchbaseSchema.Queries {

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
  def bucketName: String = settings.bucket
  val n1qlQueryStageSettings = N1qlQueryStage.N1qlQuerySettings(settings.liveQueryInterval, settings.pageSize)

  protected val asyncSession: Future[CouchbaseSession] =
    CouchbaseSessionRegistry(system).sessionFor(settings.sessionSettings, settings.bucket)
  asyncSession.failed.foreach { ex =>
    log.error(ex, "Failed to connect to couchbase")
  }

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
      val deletedTo: Future[Long] =
        firstNonDeletedEventFor(persistenceId, session, 10.seconds) // FIXME timeout from config
          .map(_.getOrElse(fromSequenceNr))

      val source = deletedTo.map { startFrom =>
        if (log.isDebugEnabled)
          log.debug(
            "events by persistenceId: live {}, persistenceId: {}, from: {}, actualFrom: {}, to: {}",
            Array(live, persistenceId, fromSequenceNr, startFrom, toSequenceNr)
          )
        Source
          .fromGraph(
            new N1qlQueryStage[Long](
              live,
              n1qlQueryStageSettings,
              eventsByPersistenceIdQuery(persistenceId, startFrom, toSequenceNr, settings.pageSize),
              session.underlying,
              startFrom, { from =>
                if (from <= toSequenceNr)
                  Some(eventsByPersistenceIdQuery(persistenceId, from, toSequenceNr, settings.pageSize))
                else None
              },
              (_, row) => row.value().getLong(Fields.SequenceNr) + 1
            )
          )
          .mapMaterializedValue(_ => NotUsed)
      }

      Source
        .fromFutureSource(source)
        .mapAsync(1) { row: AsyncN1qlQueryRow =>
          CouchbaseSchema
            .deserializeEvent(row.value(), serialization)
            .map(tpr => EventEnvelope(Offset.sequence(tpr.sequenceNr), tpr.persistenceId, tpr.sequenceNr, tpr.payload))
        }
        .mapMaterializedValue(_ => NotUsed)
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

      // note that the result is "unnested" into the message objects + the persistence id, so the normal
      // document structure does not apply here
      val taggedRows: Source[AsyncN1qlQueryRow, NotUsed] =
        Source
          .fromGraph(
            new N1qlQueryStage[String](
              live,
              n1qlQueryStageSettings,
              eventsByTagQuery(tag, initialOrderingString, endOffset, settings.pageSize),
              session.underlying,
              initialOrderingString, { ordering =>
                Some(eventsByTagQuery(tag, ordering, endOffset, settings.pageSize))
              }, { (_, row) =>
                row.value().getString(Fields.Ordering)
              }
            )
          )

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
    log.debug("currentPersistenceIds query")
    // FIXME paging & respect page size for this query as well? #108
    session.streamedQuery(persistenceIdsQuery()).map(_.getString(Fields.PersistenceId))
  }

  override def persistenceIds(): Source[String, NotUsed] = sourceWithCouchbaseSession { session =>
    log.debug("persistenceIds query")
    Source
      .fromGraph(
        new N1qlQueryStage[NotUsed](
          live = true,
          n1qlQueryStageSettings,
          persistenceIdsQuery(),
          session.underlying,
          NotUsed,
          nextQuery = _ => Some(persistenceIdsQuery()),
          (_, _) => NotUsed
        )
      )
      .statefulMapConcat[String] { () =>
        var seenIds = Set.empty[String]

        { (row) =>
          val id = row.value().getString(Fields.PersistenceId)
          if (seenIds.contains(id)) Nil
          else {
            seenIds += id
            id :: Nil
          }
        }
      }

  }
}
