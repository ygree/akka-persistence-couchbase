/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.Base64

import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.couchbase.CouchbaseJournal.Fields
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{ JsonArray, JsonObject }
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.couchbase.client.java.{ Cluster, CouchbaseCluster }
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.{ immutable => im }
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

object CouchbaseJournal {

  case class TaggedPersistentRepr(pr: PersistentRepr, tags: Set[String])

  def deserialize[T](value: JsonObject, toSequenceNr: Long, serialization: Serialization,
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
    PersistentRepr(
      payload = payload,
      sequenceNr = sequenceNr,
      persistenceId = pid,
      writerUuid = writerUuid)
  }

  def extractTaggedEvent(pid: String, writerUuid: String, event: JsonObject, serialization: Serialization): TaggedPersistentRepr = {
    val serManifest = event.getString(Fields.SerializerManifest)
    val serId = event.getInt(Fields.SerializerId)
    val bytes = Base64.getDecoder.decode(event.getString(Fields.Payload))
    val payload = serialization.deserialize(bytes, serId, serManifest).get // hrmm
    val sequenceNr = event.getLong(Fields.SequenceNr)
    val tags: Set[AnyRef] = event.getArray("tags").toList.asScala.toSet
    CouchbaseJournal.TaggedPersistentRepr(PersistentRepr(
      payload = payload,
      sequenceNr = sequenceNr,
      persistenceId = pid,
      writerUuid = writerUuid), tags.map(_.toString))
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

}

/*

Need at least this GI:
- create index pi2  on akka(akka.persistenceId, akka.sequence_from)

 */
class CouchbaseJournal(c: Config, configPath: String) extends AsyncWriteJournal {

  private implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(this)
  private val serialization: Serialization = SerializationExtension(context.system)

  private implicit val materializer = ActorMaterializer() // FIXME keeping our own rather than reusing another one?

  // FIXME move connect logic to connector/client?
  private val config: CouchbaseSettings = CouchbaseSettings(c)
  private val cluster: Cluster = {
    val c = CouchbaseCluster.create()
    log.info("Auth {} {}", config.username, config.password)
    c.authenticate(config.username, config.password)
    c
  }

  val couchbase = CouchbaseSession(cluster.openBucket(config.bucket))

  override def postStop(): Unit = {
    couchbase.close()
  }

  override def asyncWriteMessages(messages: im.Seq[AtomicWrite]): Future[im.Seq[Try[Unit]]] = {
    log.info("asyncWriteMessages {}", messages)
    require(messages.nonEmpty)
    val pid = messages.head.persistenceId
    val inserts: im.Seq[(String, JsonObject)] = messages.map(aw => {
      var allTags = Set.empty[String]
      val serialized: im.Seq[JsonObject] = aw.payload.map { msg =>
        val (event, tags) = msg.payload match {
          case t: Tagged => (t.payload.asInstanceOf[AnyRef], t.tags)
          case other     => (other.asInstanceOf[AnyRef], Set.empty)
        }
        allTags = allTags ++ tags
        val ser = Serialized.serialize(serialization, event)
        ser.asJson()
          .put("tags", JsonArray.from(tags.toList.asJava))
          .put(Fields.SequenceNr, msg.sequenceNr)
      }

      val insert: JsonObject = JsonObject.create()
        .put("type", "journal_message")
        // assumed all msgs have the same writerUuid
        .put("writer_uuid", aw.payload.head.writerUuid.toString)
        .put(Fields.PersistenceId, pid)
        .put(Fields.SequenceFrom, aw.lowestSequenceNr)
        .put(Fields.SequenceTo, aw.highestSequenceNr)
        .put("messages", JsonArray.from(serialized.asJava))
        .put("all_tags", JsonArray.from(allTags.toList.asJava))
      (s"$pid-${aw.lowestSequenceNr}", insert)
    })

    // FIXME sequence will fail entire future rather than individual write
    Future.sequence(inserts.map {
      case (persistenceId, json) =>
        val p = Promise[Try[Unit]]()
        // TODO make persistTo configurable
        couchbase.counter(config.bucket, 1, 0).flatMap { id =>
          val withId = json.put(Fields.Ordering, id)
          couchbase.insert(JsonDocument.create(persistenceId, withId))
        }
    }).map(writes => writes.map(_ => Success(())))
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.info("asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr)
    val docId = s"$persistenceId-meta"
    (if (toSequenceNr == Long.MaxValue) {
      log.info("Journal cleanup (Long.MaxValue)")
      asyncReadHighestSequenceNr(persistenceId, 0).flatMap { highestSeqNr =>
        couchbase.upsert(JsonDocument.create(docId, JsonObject.create().put("deleted_to", highestSeqNr)))
      }
    } else {
      couchbase.upsert(JsonDocument.create(docId, JsonObject.create().put("deleted_to", toSequenceNr)))
    }).map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  // TODO use eventsByPersistenceId
  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    log.info("asyncReplayMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)

    val deletedTo: Future[Long] =
      couchbase.get(s"$persistenceId-meta")
        .map {
          case Some(jsonDoc) =>
            val dt = jsonDoc.content().getLong("deleted_to").toLong
            log.info("Previously deleted to: {}", dt)
            dt + 1 // start at the next sequence nr
          case None => fromSequenceNr
        }

    val replayFinished: Future[Unit] = deletedTo.flatMap { starting =>

      log.info("Starting at sequence_nr {}", starting)

      // FIXME, needs an orderby make sure index works with it
      val replayQuery = select("*")
        .from(config.bucket)
        .where(x(Fields.PersistenceId).eq(x("$1"))
          .and(x(Fields.SequenceFrom).gte(starting)
            .and(Fields.SequenceFrom).lte(toSequenceNr)))

      log.info("Query: " + replayQuery)
      val query = N1qlQuery.parameterized(replayQuery, JsonArray.from(persistenceId))

      val source =
        if (max >= 0 && max != Long.MaxValue) couchbase.streamedQuery(query).take(max) // FIXME make sure this cancels the query
        else couchbase.streamedQuery(query)

      val done = source
        .runForeach(json =>
          CouchbaseJournal.deserialize(json.getObject(config.bucket), toSequenceNr, serialization, CouchbaseJournal.extractEvent).foreach { pr =>
            recoveryCallback(pr)
          })

      done.onComplete {
        case Failure(ex) =>
          // FIXME this is just to see errors in development?
          ex.printStackTrace(System.out)
        case _ =>
      }
      done.map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
    }

    replayFinished
  }

  // TODO how horrific is this query?
  // select persistenceId, sequence_from from akka where akka.persistenceId = "pid1" order by sequence_from desc limit 1
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.info("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)

    val highestSequenceNrStatement = select(Fields.SequenceTo)
      .from(config.bucket)
      .where(x(Fields.PersistenceId).eq(x("$1"))
        .and(x(Fields.SequenceFrom).gte(fromSequenceNr)))
      .orderBy(desc(Fields.SequenceFrom))
      .limit(1)

    // Without this the index that gets the latest sequence nr may have not seen the last write of the last version
    // of this persistenceId. This seems overkill.
    val params: N1qlParams = N1qlParams.build()
      .consistency(ScanConsistency.REQUEST_PLUS)

    val highestSequenceNrQuery = N1qlQuery.parameterized(highestSequenceNrStatement, JsonArray.from(persistenceId), params)

    log.info("Executing: {}", highestSequenceNrQuery)

    couchbase.singleResponseQuery(highestSequenceNrQuery)
      .map {
        case Some(jsonObj) =>
          log.info("sequence nr: {}", jsonObj)
          jsonObj.getLong("sequence_to")
        case None =>
          0L
      }
  }
}
