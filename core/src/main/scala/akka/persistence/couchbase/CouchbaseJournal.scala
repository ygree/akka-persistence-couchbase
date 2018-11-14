/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.Base64

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.typesafe.config.Config
import rx.Observable
import rx.functions.Func1

import scala.collection.JavaConverters._
import scala.collection.{immutable => im}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object CouchbaseJournal {

  case class TaggedPersistentRepr(pr: PersistentRepr, tags: Set[String])

  def deserialize[T](value: JsonObject,
                     toSequenceNr: Long,
                     serialization: Serialization,
                     extract: (String, String, JsonObject, Serialization) => T): im.Seq[T] = {
    val persistenceId = value.getString(Fields.PersistenceId)
    val from: Long = value.getLong(Fields.SequenceFrom)
    val sequenceTo: Long = value.getLong(Fields.SequenceTo)
    val localTo: Long = if (sequenceTo > toSequenceNr) toSequenceNr else sequenceTo
    val events: JsonArray = value.getArray("messages")
    val writerUuid = value.getString("writer_uuid")
    require(sequenceTo - from + 1 == events.size())
    val nrReply = localTo - from
    (0 to nrReply.asInstanceOf[Int]) map { i =>
      extract(persistenceId, writerUuid, events.getObject(i), serialization)
    }
  }

  def extractEvent(pid: String, writerUuid: String, event: JsonObject, serialization: Serialization): PersistentRepr = {
    val payload = Serialized.fromJsonObject(serialization, event)
    val sequenceNr = event.getLong(Fields.SequenceNr)
    PersistentRepr(payload = payload, sequenceNr = sequenceNr, persistenceId = pid, writerUuid = writerUuid)
  }

  def extractTaggedEvent(pid: String,
                         writerUuid: String,
                         event: JsonObject,
                         serialization: Serialization): TaggedPersistentRepr = {
    val serManifest = event.getString(Fields.SerializerManifest)
    val serId = event.getInt(Fields.SerializerId)
    val bytes = Base64.getDecoder.decode(event.getString(Fields.Payload))
    val payload = serialization.deserialize(bytes, serId, serManifest).get // hrmm
    val sequenceNr = event.getLong(Fields.SequenceNr)
    val tags: Set[AnyRef] = event.getArray("tags").toList.asScala.toSet
    CouchbaseJournal.TaggedPersistentRepr(
      PersistentRepr(payload = payload, sequenceNr = sequenceNr, persistenceId = pid, writerUuid = writerUuid),
      tags.map(_.toString)
    )
  }

  object Fields {
    val Type = "type"

    val PersistenceId = "persistence_id"
    val SequenceNr = "sequence_nr"
    val SequenceFrom = "sequence_from"
    val SequenceTo = "sequence_to"
    val Ordering = "ordering"
    val Timestamp = "timestamp"
    val Payload = "payload"

    val SerializerManifest = "ser_manifest"
    val SerializerId = "ser_id"
  }

  private val ExtraSuccessFulUnit: Try[Unit] = Success(())

}

/**
 * INTERNAL API
 *
 * Need at least this GI:
 * - create index pi2  on akka(akka.persistenceId, akka.sequence_from)
 */
@InternalApi
class CouchbaseJournal(config: Config, configPath: String) extends AsyncWriteJournal {

  import CouchbaseJournal._

  private implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(this)
  private val serialization: Serialization = SerializationExtension(context.system)
  private implicit val materializer = ActorMaterializer()

  private val settings: CouchbaseJournalSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.write$""", "")
    val sharedConfig = context.system.settings.config.getConfig(commonPath)

    CouchbaseJournalSettings(sharedConfig)
  }

  //TODO: extract these helpers to common place
  private def flatMapResult[T, R](fun: T => Observable[R]) =
    new Func1[T, Observable[R]]() {
      override def call(b: T): Observable[R] = fun(b)
    }

  private def mapResult[T, R](fun: T => R) =
    new Func1[T, R]() {
      override def call(b: T): R = fun(b)
    }

  def initSession(bucket: AsyncBucket): Observable[AsyncBucket] =
    if (settings.indexAutocreate) {

      bucket
        .bucketManager()
        .flatMap(
          flatMapResult[AsyncBucketManager, java.lang.Boolean](
            _.createN1qlIndex("pi2", true, false, "persistence_id", "sequence_from")
          )
        )
        .map(mapResult(_ => bucket))
    } else {
      Observable.just(bucket)
    }

  private val couchbase = CouchbaseSession(settings.sessionSettings, settings.bucket, initSession)

  // TODO how horrific is this query?
  // select persistenceId, sequence_from from akka where akka.persistenceId = "pid1" order by sequence_from desc limit 1
  private val highestSequenceNrStatement = select(Fields.SequenceTo)
    .from(settings.bucket)
    .where(
      x(Fields.PersistenceId)
        .eq(x("$1"))
        .and(x(Fields.SequenceFrom).gte(x("$2")))
    )
    .orderBy(desc(Fields.SequenceFrom))
    .limit(1)

  // FIXME, needs an orderby make sure index works with it
  // select * from akka where akka.persistenceId = "pid1" and sequence_from >= start and sequenceFrom <= end
  // can't be a val because we need to limit and that mutates the statement :/
  private def replayStatement =
    select("*")
      .from(settings.bucket)
      .where(
        x(Fields.PersistenceId)
          .eq(x("$1"))
          .and(
            x(Fields.SequenceFrom)
              .gte(x("$2"))
              .and(Fields.SequenceFrom)
              .lte(x("$3"))
          )
      )

  override def postStop(): Unit = {
    couchbase.close()
    materializer.shutdown()
  }

  override def asyncWriteMessages(messages: im.Seq[AtomicWrite]): Future[im.Seq[Try[Unit]]] = {
    log.debug("asyncWriteMessages {}", messages)
    require(messages.nonEmpty)
    val pid = messages.head.persistenceId
    val docsToInsert: im.Seq[JsonDocument] = messages.map(aw => {
      var allTags = Set.empty[String]
      val serialized: im.Seq[JsonObject] = aw.payload.map { msg =>
        val (event, tags) = msg.payload match {
          case t: Tagged => (t.payload.asInstanceOf[AnyRef], t.tags)
          case other => (other.asInstanceOf[AnyRef], Set.empty[String])
        }
        allTags = allTags ++ tags
        val serialized = Serialized.serialize(serialization, event)
        serialized
          .asJson()
          .put("tags", JsonArray.from(tags.toList.asJava))
          .put(Fields.SequenceNr, msg.sequenceNr)
      }

      val insert: JsonObject = JsonObject
        .create()
        .put("type", "journal_message")
        // assumed all msgs have the same writerUuid
        .put("writer_uuid", aw.payload.head.writerUuid.toString)
        .put(Fields.PersistenceId, pid)
        .put(Fields.SequenceFrom, aw.lowestSequenceNr)
        .put(Fields.SequenceTo, aw.highestSequenceNr)
        .put("messages", JsonArray.from(serialized.asJava))
        .put("all_tags", JsonArray.from(allTags.toList.asJava))
        // FIXME temporary ordering solution #66
        .put(Fields.Ordering, System.currentTimeMillis())
      JsonDocument.create(s"$pid-${aw.lowestSequenceNr}", insert)
    })

    val inserts: im.Seq[Future[Try[Unit]]] = docsToInsert.map(
      doc =>
        couchbase
          .insert(doc, settings.writeSettings)
          .map(json => ExtraSuccessFulUnit)(ExecutionContexts.sameThreadExecutionContext)
          .recover {
            // lift the failure to not fail the whole sequence
            case NonFatal(ex) => Failure(ex)
          }(ExecutionContexts.sameThreadExecutionContext)
    )
    Future.sequence(inserts)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr)
    val docId = s"$persistenceId-meta"
    (if (toSequenceNr == Long.MaxValue) {
       log.debug("Journal cleanup (Long.MaxValue)")
       asyncReadHighestSequenceNr(persistenceId, 0).flatMap { highestSeqNr =>
         val doc = JsonDocument.create(docId, JsonObject.create().put("deleted_to", highestSeqNr))
         couchbase.upsert(doc, settings.writeSettings)
       }
     } else {
       val doc = JsonDocument.create(docId, JsonObject.create().put("deleted_to", toSequenceNr))
       couchbase.upsert(doc, settings.writeSettings)
     }).map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug("asyncReplayMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)

    val deletedTo: Future[Long] =
      couchbase
        .get(s"$persistenceId-meta", settings.readTimeout)
        .map {
          case Some(jsonDoc) =>
            val dt = jsonDoc.content().getLong("deleted_to").toLong
            log.debug("Previously deleted to: {}", dt)
            dt + 1 // start at the next sequence nr
          case None => fromSequenceNr
        }

    val replayFinished: Future[Unit] = deletedTo.flatMap { starting =>
      log.debug("Starting at sequence_nr {}, query: {}", starting, replayStatement)

      val limitedStatement =
        if (max >= 0 && max != Long.MaxValue) {
          if (max > Int.MaxValue.toLong) {
            throw new IllegalArgumentException(s"Couchbase cannot limit replay at more than ${Int.MaxValue}, got $max")
          }
          replayStatement.limit(max.toInt)
        } else replayStatement

      val query =
        N1qlQuery.parameterized(limitedStatement,
                                JsonArray.from(persistenceId, starting: java.lang.Long, toSequenceNr: java.lang.Long))
      val source = couchbase.streamedQuery(query)

      source
        .runForeach(
          json =>
            CouchbaseJournal
              .deserialize(json.getObject(settings.bucket), toSequenceNr, serialization, CouchbaseJournal.extractEvent)
              .foreach { pr =>
                recoveryCallback(pr)
            }
        )
        .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
    }

    replayFinished
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)

    val highestSequenceNrQuery = N1qlQuery.parameterized(
      highestSequenceNrStatement,
      JsonArray.from(persistenceId, fromSequenceNr: java.lang.Long),
      // Without this the index that gets the latest sequence nr may have not seen the last write of the last version
      // of this persistenceId. This seems overkill.
      N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)
    )

    log.debug("Executing: {}", highestSequenceNrQuery)

    couchbase
      .singleResponseQuery(highestSequenceNrQuery)
      .map {
        case Some(jsonObj) =>
          log.debug("sequence nr: {}", jsonObj)
          jsonObj.getLong("sequence_to")
        case None =>
          0L
      }
  }
}
