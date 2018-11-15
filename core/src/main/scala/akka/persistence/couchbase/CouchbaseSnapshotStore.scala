/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.couchbase.CouchbaseSchema.Fields
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
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
  private implicit val system: ActorSystem = context.system
  private implicit val ec: ExecutionContext = context.dispatcher

  val couchbase = CouchbaseSession.Holder(settings.sessionSettings, settings.bucket)

  val serialization = SerializationExtension(context.system)

  override def postStop(): Unit =
    couchbase.close()

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
      .from(settings.bucket)
      .where(filter)
      .orderBy(desc(Fields.SequenceNr))
      .limit(1)

    // FIXME, deal with errors
    val result: Future[Option[JsonObject]] = couchbase.withCouchbase(
      _.singleResponseQuery(
        N1qlQuery.parameterized(query,
                                JsonArray.from("snapshot", persistenceId),
                                N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))
      )
    )

    result.flatMap {
      case Some(snapshot) =>
        val value = snapshot.getObject(settings.bucket)
        SerializedMessage.fromJsonObject(serialization, value).map { snapshot =>
          Some(
            SelectedSnapshot(
              SnapshotMetadata(persistenceId, value.getLong(Fields.SequenceNr), value.getLong(Fields.Timestamp)),
              snapshot
            )
          )
        }
      case None => Future.successful(None)
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val ser: Future[SerializedMessage] = SerializedMessage.serialize(serialization, snapshot.asInstanceOf[AnyRef])

    ser.map { serializedMessage =>
      val toStore: JsonObject =
        CouchbaseSchema
          .serializedMessageToObject(serializedMessage)
          .put(Fields.Type, CouchbaseSchema.SnapshotEntryType)
          .put(Fields.Timestamp, metadata.timestamp)
          .put(Fields.SequenceNr, metadata.sequenceNr)
          .put(Fields.PersistenceId, metadata.persistenceId)

      couchbase.withCouchbase(
        _.upsert(JsonDocument.create(snapshotKey(metadata), toStore))
          .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
      )
    }
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val key = snapshotKey(metadata)
    couchbase.withCouchbase(
      _.remove(key)
        .recover {
          case _: DocumentDoesNotExistException => ()
        }
        .map(_ => ())
    )
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val filter = snapshotFilter(criteria)
    val query = N1qlQuery.parameterized(
      deleteFrom(settings.bucket)
        .where(filter),
      JsonArray.from(CouchbaseSchema.SnapshotEntryType, persistenceId),
      N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)
    )

    couchbase.withCouchbase(_.singleResponseQuery(query).map(_ => ()))
  }

  private def snapshotFilter(criteria: SnapshotSelectionCriteria): Expression = {
    var filter = x(s"${settings.bucket}.${Fields.Type}")
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
