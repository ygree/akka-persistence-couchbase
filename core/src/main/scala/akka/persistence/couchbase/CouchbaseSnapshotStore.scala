/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.dispatch.ExecutionContexts
import akka.persistence.couchbase.CouchbaseJournal.Fields
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.error.DocumentDoesNotExistException
import com.couchbase.client.java.query.Delete._
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.couchbase.client.java.query.{N1qlQuery, _}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

// FIXME, share a Couchbase cluster between read/journal and snapshot
// Make it an extension to look them up?
class CouchbaseSnapshotStore(cfg: Config, configPath: String) extends SnapshotStore {

  private val settings: CouchbaseSnapshotSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.snapshot""", "")
    val sharedConfig = context.system.settings.config.getConfig(commonPath)

    CouchbaseSnapshotSettings(sharedConfig)
  }
  private implicit val ec: ExecutionContext = context.dispatcher

  val couchbase =
    CouchbaseSessionFactory(context.system, settings.sessionSettings, settings.bucket, settings.indexAutoCreate)

  val serialization = SerializationExtension(context.system)

  /**
   * select * from akka where type = "snapshot"
   * and persistence_id = "p-1"
   * and sequence_nr <= 100
   * and sequence_nr >= 0
   * and timestamp <= 1635974897888
   * order by sequence_nr desc
   * limit 1
   *
   */
  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val filter = snapshotFilter(criteria)

    val query = select("*")
      .from("akka")
      .where(filter)
      .orderBy(desc(Fields.SequenceNr))
      .limit(1)

    // FIXME, deal with errors
    val result: Future[Option[JsonObject]] = couchbase.flatMap(
      _.singleResponseQuery(
        N1qlQuery.parameterized(query,
                                JsonArray.from("snapshot", persistenceId),
                                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
      )
    )

    result.map {
      case Some(snapshot) =>
        val value = snapshot.getObject("akka")
        Some(
          SelectedSnapshot(
            SnapshotMetadata(persistenceId, value.getLong(Fields.SequenceNr), value.getLong(Fields.Timestamp)),
            Serialized.fromJsonObject(serialization, value)
          )
        )
      case None => None
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val ser: Serialized = Serialized.serialize(serialization, snapshot.asInstanceOf[AnyRef])

    val toStore: JsonObject = ser
      .asJson()
      .put(Fields.Type, "snapshot")
      .put(Fields.Timestamp, metadata.timestamp)
      .put(Fields.SequenceNr, metadata.sequenceNr)
      .put(Fields.PersistenceId, metadata.persistenceId)

    couchbase
      .flatMap(_.upsert(JsonDocument.create(snapshotKey(metadata), toStore)))
      .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val key = snapshotKey(metadata)
    couchbase
      .flatMap(_.remove(key))
      .recover {
        case _: DocumentDoesNotExistException => ()
      }
      .map(_ => ())
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val filter = snapshotFilter(criteria)
    val query = N1qlQuery.parameterized(deleteFrom("akka")
                                          .where(filter),
                                        JsonArray.from("snapshot", persistenceId),
                                        N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS))

    couchbase.flatMap(_.singleResponseQuery(query)).map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  private def snapshotFilter(criteria: SnapshotSelectionCriteria): Expression = {
    var filter = x("akka.type")
      .eq("$1")
      .and(x(Fields.PersistenceId).eq("$2"))

    if (criteria.maxSequenceNr != Long.MaxValue)
      filter = filter.and(x(Fields.SequenceNr).lte(criteria.maxSequenceNr))

    if (criteria.minSequenceNr != 0)
      filter = filter.and(x(Fields.SequenceNr).gte(criteria.maxSequenceNr))

    if (criteria.maxTimestamp != Long.MaxValue)
      filter = filter.and(x(Fields.Timestamp).lte(criteria.maxTimestamp))

    if (criteria.minTimestamp != 0)
      filter = filter.and(x(Fields.Timestamp).gte(criteria.minTimestamp))

    filter
  }

  private def snapshotKey(metadata: SnapshotMetadata): String =
    s"${metadata.persistenceId}-${metadata.sequenceNr}-snapshot"

}
