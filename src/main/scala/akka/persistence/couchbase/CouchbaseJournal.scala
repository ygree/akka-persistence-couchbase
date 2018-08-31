package akka.persistence.couchbase

import akka.event.Logging
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension, Serializers}
import com.couchbase.client.java.document.{JsonDocument, JsonLongDocument}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.couchbase.client.java.{AsyncBucket, Bucket, Cluster, CouchbaseCluster}
import com.typesafe.config.Config
import rx.{Observable, Subscriber}

import scala.collection.JavaConverters._
import scala.collection.{immutable => im}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import java.util.Base64

import akka.persistence.couchbase.CouchbaseJournal.Fields

object CouchbaseJournal {

  case class TaggedPersistentRepr(pr: PersistentRepr, tags: Set[String])

  def deserialize[T](row: AsyncN1qlQueryRow, toSequenceNr: Long, serialization: Serialization,
                     extract: (String, String, JsonObject, Serialization) => T): im.Seq[T] = {
    val value = row.value().getObject("akka")
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
    val serManifest = event.getString("manifest")
    val serId = event.getInt("id")
    val bytes = Base64.getDecoder.decode(event.getString("payload"))
    val payload = serialization.deserialize(bytes, serId, serManifest).get // hrmm
    val sequenceNr = event.getLong(Fields.SequenceNr)
    PersistentRepr(
      payload = payload,
      sequenceNr = sequenceNr,
      persistenceId = pid,
      writerUuid = writerUuid
    )
  }

  def extractTaggedEvent(pid: String, writerUuid: String, event: JsonObject, serialization: Serialization): TaggedPersistentRepr = {
    val serManifest = event.getString("manifest")
    val serId = event.getInt("id")
    val bytes = Base64.getDecoder.decode(event.getString("payload"))
    val payload = serialization.deserialize(bytes, serId, serManifest).get // hrmm
    val sequenceNr = event.getLong(Fields.SequenceNr)
    val tags: Set[AnyRef] = event.getArray("tags").toList.asScala.toSet
    CouchbaseJournal.TaggedPersistentRepr(PersistentRepr(
      payload = payload,
      sequenceNr = sequenceNr,
      persistenceId = pid,
      writerUuid = writerUuid
    ), tags.map(_.toString))
  }

  object Fields {
    val PersistenceId = "persistence_id"
    val SequenceNr = "sequence_nr"
    val SequenceFrom = "sequence_from"
    val SequenceTo = "sequence_to"
    val Ordering = "ordering"
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

  private val config: CouchbaseSettings = CouchbaseSettings(c)

  private val cluster: Cluster = {
    val c = CouchbaseCluster.create()
    log.info("Auth {} {}", config.username, config.password)
    c.authenticate(config.username, config.password)
    c
  }

  val bucket: Bucket = cluster.openBucket(config.bucket)
  val asyncBucket: AsyncBucket = cluster.openBucket(config.bucket).async()

  override def postStop(): Unit = {
    cluster.disconnect()
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
          case other => (other.asInstanceOf[AnyRef], Set.empty)
        }
        allTags = allTags ++ tags
        val m = JsonObject.create()
        val serializer = serialization.findSerializerFor(event)
        val serManifest = Serializers.manifestFor(serializer, event)
        val serId = serializer.identifier
        m.put("manifest", serManifest)
        m.put("id", serId)
        val bytes: String = Base64.getEncoder.encodeToString(serialization.serialize(event).get) // TODO deal with failure
        m.put("payload", bytes)
        m.put("tags", JsonArray.from(tags.toList.asJava))
        m.put(Fields.SequenceNr, msg.sequenceNr)
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


    val result = Future.sequence(inserts.map((jo: (String, JsonObject)) => {
      val p = Promise[Try[Unit]]()
      // TODO make persistTo configurable
      val something: Observable[JsonLongDocument] = asyncBucket.counter("akka", 1, 0)

      val write = something.flatMap(id => {
        val withId = jo._2.put(Fields.Ordering, id.content())
        asyncBucket.insert(JsonDocument.create(jo._1, withId))
      })


      write.single().subscribe(new Subscriber[JsonDocument]() {
        override def onCompleted(): Unit = p.tryComplete(Try(Try(())))
        override def onError(e: Throwable): Unit = p.tryFailure(e)
        override def onNext(t: JsonDocument): Unit = ()
      })
      p.future
    }))
    result
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.info("asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr)
    val docId = s"$persistenceId-meta"
    if (toSequenceNr == Long.MaxValue) {
      log.info("Journal cleanup (Long.MaxValue)")
      asyncReadHighestSequenceNr(persistenceId, 0).flatMap { highestSeqNr =>
        singleObservableToFuture(asyncBucket.upsert(JsonDocument.create(docId, JsonObject.create().put("deleted_to", highestSeqNr))))
          .map(_ => ())
      }
    } else {
      singleObservableToFuture(asyncBucket.upsert(JsonDocument.create(docId, JsonObject.create().put("deleted_to", toSequenceNr))))
        .map(_ => ())
    }
  }

  private def singleObservableToFuture[T](o: Observable[T]): Future[T] = {
    val p = Promise[T]
    o.single()
      .subscribe(new Subscriber[T]() {
        override def onCompleted(): Unit = ()
        override def onError(e: Throwable): Unit = p.tryFailure(e)
        override def onNext(t: T): Unit = p.tryComplete(Try(t))
      })
    p.future
  }

  private def zeroOrOneObservableToFuture[T](o: Observable[T]): Future[Option[T]] = {
    val p = Promise[Option[T]]
    o.subscribe(new Subscriber[T]() {
      override def onCompleted(): Unit = p.tryComplete(Try(None))
      override def onError(e: Throwable): Unit = p.tryFailure(e)
      override def onNext(t: T): Unit = p.tryComplete(Try(Some(t)))
    })
    p.future
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    log.info("asyncReplayMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)
    val deletedTo: Future[Long] = zeroOrOneObservableToFuture(asyncBucket.get(s"$persistenceId-meta"))
      .map {
        case Some(jo: JsonDocument) =>
          val dt = jo.content().getLong("deleted_to").toLong
          log.info("Previously deleted to: {}", dt)
          dt + 1 // start at the next sequence nr
        case None => fromSequenceNr
      }

    val replayFinished: Future[Unit] = deletedTo.flatMap { starting =>

      log.info("Starting at sequence_nr {}", starting)

      val what = select("*")
        .from("akka")
        .where(x(Fields.PersistenceId).eq(x("$1"))
          .and(x(Fields.SequenceFrom).gte(starting)
            .and(Fields.SequenceFrom).lte(toSequenceNr)))

      log.info("Query: " + what)

      def limit[T](in: Observable[T]): Observable[T] = {
        if (max >= 0 && max != Long.MaxValue)
          in.limit(max.asInstanceOf[Int]) // TODO: Does this cancel the query? // FIXME why is this a bloody int? create a subscriber to do this
        else
          in
      }

      val query = N1qlQuery.parameterized(what, JsonArray.from(persistenceId))
      val done = Promise[Unit]

      limit(asyncBucket.query(query)
        .flatMap(result => result.rows()))
        .subscribe(
          new Subscriber[AsyncN1qlQueryRow]() {
            override def onCompleted(): Unit = {
              done.tryComplete(Try(()))
            }
            override def onError(e: Throwable): Unit = {
              e.printStackTrace(System.out)
              done.tryFailure(e)
            }
            override def onNext(row: AsyncN1qlQueryRow): Unit = {
              CouchbaseJournal.deserialize(row, toSequenceNr, serialization, CouchbaseJournal.extractEvent).foreach { pr =>
                recoveryCallback(pr)
              }
            }
          }
        )
      done.future
    }

    replayFinished
  }

  // TODO how horrific is this query?
  // select persistenceId, sequence_from from akka where akka.persistenceId = "pid1" order by sequence_from desc limit 1
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.info("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)

    val result = Promise[Long]

    val highestSequenceNrStatement = select(Fields.SequenceTo)
      .from("akka")
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

    asyncBucket.query(highestSequenceNrQuery)
      .flatMap(result => result.rows())
      .subscribe(new Subscriber[AsyncN1qlQueryRow]() {
        override def onCompleted(): Unit = {
          if (!result.isCompleted)
            result.tryComplete(Try(fromSequenceNr))

          log.info("Highest sequence nr query complete. {}", result.future.value)
          ()
        }

        override def onError(e: Throwable): Unit = result.tryFailure(e)

        override def onNext(t: AsyncN1qlQueryRow): Unit = {
          log.info("sequence nr: {}", t)
          val to = t.value().getLong("sequence_to")
          result.tryComplete(Try(to))
        }

      })

    result.future
  }
}
