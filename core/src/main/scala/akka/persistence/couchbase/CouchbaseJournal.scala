/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.typesafe.config.Config

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

  private val ExtraSuccessFulUnit: Try[Unit] = Success(())

}

/**
 * INTERNAL API
 */
@InternalApi
class CouchbaseJournal(config: Config, configPath: String) extends AsyncWriteJournal {

  import CouchbaseJournal._
  import CouchbaseSchema.Fields

  private implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(this)
  private implicit val system = context.system
  private val serialization: Serialization = SerializationExtension(system)
  private implicit val materializer = ActorMaterializer()(context)

  private val settings: CouchbaseJournalSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.write$""", "")
    val sharedConfig = context.system.settings.config.getConfig(commonPath)

    CouchbaseJournalSettings(sharedConfig)
  }

  private val couchbase = Couchbase(settings.sessionSettings, settings.bucket, settings.indexAutoCreate)

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
    val docsToInsert: Future[im.Seq[JsonDocument]] =
      Future.sequence(messages.map(atomicWriteToJsonDoc)).andThen {
        case Failure(ex) => ex.printStackTrace()
        case _ =>
      }

    docsToInsert.flatMap(docs => Future.sequence(docs.map(insertJsonDoc)))
  }

  private def insertJsonDoc(jsonDoc: JsonDocument): Future[Try[Unit]] =
    couchbase.mapToFuture(
      _.insert(jsonDoc, settings.writeSettings)
        .map(json => ExtraSuccessFulUnit)(ExecutionContexts.sameThreadExecutionContext)
        .recover {
          // lift the failure to not fail the whole sequence
          case NonFatal(ex) => Failure(ex)
        }(ExecutionContexts.sameThreadExecutionContext)
    )

  private def atomicWriteToJsonDoc(write: AtomicWrite): Future[JsonDocument] = {
    val serializedMessages: Future[Seq[JsonObject]] = Future.sequence(write.payload.map { persistentRepr =>
      val (event, tags) = persistentRepr.payload match {
        case t: Tagged => (t.payload.asInstanceOf[AnyRef], t.tags)
        case other => (other.asInstanceOf[AnyRef], Set.empty[String])
      }
      SerializedMessage.serialize(serialization, event).map { message =>
        CouchbaseSchema
          .serializedMessageToObject(message)
          .put(Fields.Tags, JsonArray.from(tags.toList.asJava))
          .put(Fields.SequenceNr, persistentRepr.sequenceNr)
      }
    })
    serializedMessages.map { messagesJson =>
      val pid = write.persistenceId
      // collect all tags for the events and put in the surrounding
      // object for simple indexing on tags
      val allTags = write.payload.iterator
        .map(_.payload)
        .collect {
          case t: Tagged => t.tags
        }
        .flatten
        .toSet
      val insert: JsonObject = JsonObject
        .create()
        .put(Fields.Type, CouchbaseSchema.JournalEntryType)
        // assumed all msgs have the same writerUuid
        .put(Fields.WriterUuid, write.payload.head.writerUuid.toString)
        .put(Fields.PersistenceId, pid)
        .put(Fields.SequenceFrom, write.lowestSequenceNr)
        .put(Fields.SequenceTo, write.highestSequenceNr)
        .put(Fields.Messages, JsonArray.from(messagesJson.asJava))
        .put(Fields.AllTags, JsonArray.from(allTags.toList.asJava))
        // FIXME temporary ordering solution #66
        .put(Fields.Ordering, System.currentTimeMillis())
      JsonDocument.create(s"$pid-${write.lowestSequenceNr}", insert)
    }(ExecutionContexts.sameThreadExecutionContext)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr)
    val newMetadataEntry: Future[JsonDocument] =
      if (toSequenceNr == Long.MaxValue) {
        log.debug("Journal cleanup (Long.MaxValue)")
        asyncReadHighestSequenceNr(persistenceId, 0).map { highestSeqNr =>
          CouchbaseSchema.metadataEntry(persistenceId, highestSeqNr)
        }
      } else Future.successful(CouchbaseSchema.metadataEntry(persistenceId, toSequenceNr))

    newMetadataEntry
      .flatMap(entry => couchbase.mapToFuture(_.upsert(entry, settings.writeSettings)))
      .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug("asyncReplayMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)

    val deletedTo: Future[Long] =
      couchbase.mapToFuture(
        _.get(CouchbaseSchema.metadataIdFor(persistenceId), settings.readTimeout)
          .map {
            case Some(jsonDoc) =>
              val dt = jsonDoc.content().getLong(Fields.DeletedTo).toLong
              log.debug("Previously deleted to: {}", dt)
              dt + 1 // start at the next sequence nr
            case None => fromSequenceNr
          }
      )

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

      val source: Source[JsonObject, NotUsed] = couchbase.mapToSource(_.streamedQuery(query))

      val complete = source
        .mapAsync(1)(
          json =>
            CouchbaseSchema
              .deserializeEvents(json.getObject(settings.bucket),
                                 toSequenceNr,
                                 serialization,
                                 CouchbaseSchema.extractEvent)
        )
        .mapConcat(identity)
        .runForeach { pr =>
          recoveryCallback(pr)
        }
        .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)

      complete.onComplete {
        // For debugging while developing
        case Failure(ex) => log.error(ex, "Replay error for [{}]", persistenceId)
        case _ =>
      }

      complete
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

    couchbase.mapToFuture(
      _.singleResponseQuery(highestSequenceNrQuery)
        .map {
          case Some(jsonObj) =>
            log.debug("sequence nr: {}", jsonObj)
            jsonObj.getLong(Fields.SequenceTo)
          case None =>
            0L
        }
    )
  }
}
