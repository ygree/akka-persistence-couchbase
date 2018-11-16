/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.Base64

import akka.Done
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.PersistentRepr
import akka.persistence.couchbase.CouchbaseJournal.TaggedPersistentRepr
import akka.serialization.{AsyncSerializer, Serialization, Serializers}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.util.OptionVal
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.Select.select

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/*
 * INTERNAL API
 */
@InternalApi
private[akka] final object CouchbaseSchema {

  object Fields {
    val Type = "type"

    // journal fields
    /*
       Sample doc structure:
       {
         "ordering": 1542205420991,
         "sequence_to": 1,
         "all_tags": [],
         "messages": [
           {
             "sequence_nr": 1,
             "ser_id": 1,
             "ser_manifest": "",
             // either of these two depending on serializer
             "payload_bin": "rO0ABXQACHAyLWV2dC0x",
             "payload": { json- }
             "tags": []
           }
         ],
         "writer_uuid": "d61a4212-518a-4f75-8f27-2150f56ae60f",
         "type": "journal_message",
         "sequence_from": 1,
         "persistence_id": "p2"
       }
     */
    val PersistenceId = "persistence_id"
    val SequenceNr = "sequence_nr"
    // when a multi message insert is done, this will contain the sequence number range
    val SequenceFrom = "sequence_from"
    val SequenceTo = "sequence_to"
    val Ordering = "ordering"
    val Timestamp = "timestamp"
    // separate fields for json and base64 bin payloads
    val JsonPayload = "payload"
    val BinaryPayload = "payload_bin"
    val WriterUuid = "writer_uuid"
    val Messages = "messages"
    // the specific tags on an individual message
    val Tags = "tags"
    // union of all tags on an atomic write with multiple events
    val AllTags = "all_tags"

    // metadata object fields
    /*
       Sample doc structure:
       {
         "type": "journal_metadata",
         "deleted_to": 123
       }

     */
    val DeletedTo = "deleted_to"

    /*
      Snapshot doc sample:
      {
        "sequence_nr": 15,
        "ser_manifest": "",
        // either of these two depending on serializer
        "payload_bin": "rO0ABXQACHAyLWV2dC0x",
        "payload": { json- }
        "ser_id": 1,
        "type": "snapshot",
        "timestamp": 1542205413616,
        "persistence_id": "p-1"
      }
     */

    val SerializerManifest = "ser_manifest"
    val SerializerId = "ser_id"
  }

  val MetadataEntryType = "journal_metadata"
  val JournalEntryType = "journal_message"
  val SnapshotEntryType = "snapshot"

  def metadataIdFor(persistenceId: String): String = s"$persistenceId-meta"

  def metadataEntry(persistenceId: String, deletedTo: Long): JsonDocument =
    JsonDocument.create(
      metadataIdFor(persistenceId),
      JsonObject
        .create()
        .put(Fields.Type, CouchbaseSchema.JournalEntryType)
        .put(Fields.DeletedTo, deletedTo)
    )

  def serializedMessageToObject(msg: SerializedMessage): JsonObject = {
    val json = JsonObject
      .create()
      .put(Fields.SerializerManifest, msg.manifest)
      .put(Fields.SerializerId, msg.identifier)

    msg.nativePayload match {
      case OptionVal.None =>
        json.put(Fields.BinaryPayload, Base64.getEncoder.encodeToString(msg.payload))

      case OptionVal.Some(jsonPayload) =>
        json.put(Fields.JsonPayload, jsonPayload)
    }

    json
  }

  def deserializeEvents[T](
      value: JsonObject,
      toSequenceNr: Long,
      serialization: Serialization,
      extract: (String, String, JsonObject, Serialization) => Future[T]
  )(implicit ec: ExecutionContext): Future[immutable.Seq[T]] = {
    val persistenceId = value.getString(Fields.PersistenceId)
    val from: Long = value.getLong(Fields.SequenceFrom)
    val sequenceTo: Long = value.getLong(Fields.SequenceTo)
    val localTo: Long = if (sequenceTo > toSequenceNr) toSequenceNr else sequenceTo
    val events: JsonArray = value.getArray(Fields.Messages)
    val writerUuid = value.getString(Fields.WriterUuid)
    require(sequenceTo - from + 1 == events.size())
    val nrReply = localTo - from
    Future.sequence(
      (0 to nrReply.asInstanceOf[Int]).map { i =>
        extract(persistenceId, writerUuid, events.getObject(i), serialization)
      }.toVector
    )
  }

  def extractEvent(pid: String, writerUuid: String, event: JsonObject, serialization: Serialization)(
      implicit system: ActorSystem
  ): Future[PersistentRepr] = {
    val deserialized: Future[Any] = SerializedMessage.fromJsonObject(serialization, event)
    val sequenceNr = event.getLong(Fields.SequenceNr)
    deserialized.map { payload =>
      PersistentRepr(payload = payload, sequenceNr = sequenceNr, persistenceId = pid, writerUuid = writerUuid)
    }(ExecutionContexts.sameThreadExecutionContext)
  }

  def extractTaggedEvent(pid: String, writerUuid: String, event: JsonObject, serialization: Serialization)(
      implicit ec: ExecutionContext,
      system: ActorSystem
  ): Future[TaggedPersistentRepr] =
    SerializedMessage.fromJsonObject(serialization, event).map { payload =>
      val sequenceNr = event.getLong(Fields.SequenceNr)
      val tags: Set[AnyRef] = event.getArray(Fields.Tags).toList.asScala.toSet
      CouchbaseJournal.TaggedPersistentRepr(
        PersistentRepr(payload = payload, sequenceNr = sequenceNr, persistenceId = pid, writerUuid = writerUuid),
        tags.map(_.toString)
      )
    }

  // NOTE: These aren't actually used, since it seems close to impossible to guarantee the index is ready/done
  // but let's keep them around for now anyway and potentially move them into a testing utility at some point

  /**
   * Creates the indexes that the journal needs for replay
   *
   * @return true if the index is already existed, otherwise false
   */
  def createRequiredWriteJournalIndexes(session: CouchbaseSession)(implicit ec: ExecutionContext): Future[Done] =
    // CREATE INDEX `pi` ON `akka`(`persistence_id`,`sequence_from`)
    session
      .createIndex("pi", true, Fields.PersistenceId, Fields.SequenceFrom)
      .flatMap { creating =>
        if (creating) waitForIndexesToBeOnline(session)
        else Future.successful(Done)
      }

  def createRequiredReadJournalIndexes(session: CouchbaseSession)(implicit ec: ExecutionContext): Future[Done] =
    createRequiredWriteJournalIndexes(session)
      .flatMap { _ =>
        // `tags` ON `akka`((all (`all_tags`)),`ordering`);
        session.createIndex("tags", true, x(s"all (`${Fields.AllTags}`)"), Fields.Ordering)
      }
      .flatMap { creating =>
        if (creating) waitForIndexesToBeOnline(session)
        else Future.successful(Done)
      }

  private def waitForIndexesToBeOnline(session: CouchbaseSession)(implicit ec: ExecutionContext): Future[Done] =
    session
      .singleResponseQuery(select("*").from("system:indexes").where(x("status").ne(x("online"))))
      .flatMap {
        case Some(idx) =>
          println("index not ready")
          waitForIndexesToBeOnline(session)
        case None => Future.successful(Done)
      }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class SerializedMessage(identifier: Int,
                                                 manifest: String,
                                                 payload: Array[Byte],
                                                 nativePayload: OptionVal[JsonObject])

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object SerializedMessage {

  import CouchbaseSchema.Fields

  def serialize(serialization: Serialization,
                event: AnyRef)(implicit system: ActorSystem): Future[SerializedMessage] = {
    val serializer = serialization.findSerializerFor(event)
    val serManifest = Serializers.manifestFor(serializer, event)

    val serId: Int = serializer.identifier

    serializer match {
      case async: AsyncSerializer =>
        Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
          async
            .toBinaryAsync(event)
            .map(bytes => SerializedMessage(serId, serManifest, bytes, OptionVal.None))(
              ExecutionContexts.sameThreadExecutionContext
            )
        }

      case jsonSerializer: JsonSerializer =>
        Future.fromTry(
          Try(SerializedMessage(serId, serManifest, Array.emptyByteArray, OptionVal.Some(jsonSerializer.toJson(event))))
        )

      case _ =>
        Future.fromTry(Try(SerializedMessage(serId, serManifest, serialization.serialize(event).get, OptionVal.None)))
    }
  }

  def fromJsonObject(serialization: Serialization,
                     jsonObject: JsonObject)(implicit system: ActorSystem): Future[Any] = {
    val serId = jsonObject.getInt(Fields.SerializerId)
    val manifest = jsonObject.getString(Fields.SerializerManifest)

    def decodeBytes = {
      val payload = jsonObject.getString(Fields.BinaryPayload)
      Base64.getDecoder.decode(payload)
    }
    val serializer = serialization.serializerByIdentity(serId)

    serializer match {
      case async: AsyncSerializer =>
        Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
          async.fromBinaryAsync(decodeBytes, manifest)
        }

      case jsonSerializer: JsonSerializer =>
        Future.fromTry(Try(jsonSerializer.fromJson(jsonObject.getObject(Fields.JsonPayload), manifest)))

      case _ =>
        Future.fromTry(serialization.deserialize(decodeBytes, serId, manifest))
    }
  }

}
