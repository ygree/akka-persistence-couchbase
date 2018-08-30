package akka.persistence.couchbase

import akka.event.Logging
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{SerializationExtension, Serializers}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.couchbase.client.java.{AsyncBucket, Cluster, CouchbaseCluster}
import com.typesafe.config.Config
import rx.functions.{Action0, Action1}
import rx.{Observable, Subscriber}

import scala.collection.JavaConverters._
import scala.collection.{immutable => im}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import java.util.Base64
import java.nio.charset.StandardCharsets

/*

Need at least this GI:
- create index pi2  on akka(akka.persistenceId, akka.sequence_from)

 */
class CouchbaseJournal(c: Config, configPath: String) extends AsyncWriteJournal {

  private implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(this)

  val serialization = SerializationExtension(context.system)

  private val config: CouchbaseSettings = CouchbaseSettings(c)

  private val cluster: Cluster = {
    val c = CouchbaseCluster.create()
    log.info("Auth {} {}", config.username, config.password)
    c.authenticate(config.username, config.password)
    c
  }

  val bucket: AsyncBucket = cluster.openBucket(config.bucket).async()


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
        m.put("sequence_nr", msg.sequenceNr)
      }

      val insert: JsonObject = JsonObject.create()
        .put("type", "journal_message")
        // assumed all msgs have the same writerUuid
        .put("writer_uuid", aw.payload.head.writerUuid.toString)
        .put("persistenceId", pid)
        .put("sequence_from", aw.lowestSequenceNr)
        .put("sequence_to", aw.highestSequenceNr)
        .put("messages", JsonArray.from(serialized.asJava))
        .put("all_tags", JsonArray.from(allTags.toList.asJava))
      (s"$pid-${aw.lowestSequenceNr}", insert)
    })

    val huh = inserts.map(jo => {
      val p = Promise[Try[Unit]]()
      val x: Observable[JsonDocument] = bucket.insert(JsonDocument.create(jo._1, jo._2))

      x.single().subscribe((t: JsonDocument) => (), new Action1[Throwable] {
        override def call(t: Throwable): Unit = p.failure(t)
      }, new Action0 {
        override def call(): Unit = p.complete(Try(Try(())))
      })
      p.future
    })

    val result = Future.sequence(huh)
    result
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.info("asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr)
    val docId = s"$persistenceId-meta"
    if (toSequenceNr == Long.MaxValue) {
      log.info("Journal cleanup (Long.MaxValue)")
      asyncReadHighestSequenceNr(persistenceId, 0).flatMap { highestSeqNr =>
        singleObservableToFuture(bucket.upsert(JsonDocument.create(docId, JsonObject.create().put("deleted_to", highestSeqNr))))
          .map(_ => ())
      }
    } else {
      singleObservableToFuture(bucket.upsert(JsonDocument.create(docId, JsonObject.create().put("deleted_to", toSequenceNr))))
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


    val deletedTo: Future[Long] = zeroOrOneObservableToFuture(bucket.get(s"$persistenceId-meta"))
      .map {
        case Some(jo: JsonDocument) =>
          val dt = jo.content().getLong("deleted_to").toLong
          log.info("Previously deleted to: {}", dt)
          dt + 1 // start at the next sequence nr
        case None => fromSequenceNr
      }

    val sayWhat: Future[Unit] = deletedTo.flatMap { starting =>

      log.info("Starting at sequence_nr {}", starting)

      val what = select("*")
        .from("akka")
        .where(x("akka.persistenceId").eq(x("$1"))
          .and(x("akka.sequence_from").gte(starting)
            .and("akka.sequence_from").lte(toSequenceNr)))

      log.info("Query: " + what)

      def limit[T](in: Observable[T]): Observable[T] = {
        if (max >= 0 && max != Long.MaxValue)
          in.limit(max.asInstanceOf[Int]) // TODO: Does this cancel the query? // FIXME why is this a bloody int? create a subscriber to do this
        else
          in
      }

      val query = N1qlQuery.parameterized(what, JsonArray.from(persistenceId))

      val done = Promise[Unit]

      limit(bucket.query(query)
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
              val value = row.value().getObject("akka")
              val from: Long = value.getLong("sequence_from")
              val sequenceTo: Long = value.getLong("sequence_to")
              val localTo: Long = if (sequenceTo > toSequenceNr) toSequenceNr else sequenceTo
              val events: JsonArray = value.getArray("messages")
              val writerUuid = value.getString("writer_uuid")
              require(sequenceTo - from + 1 == events.size())
              val nrReply = localTo - from
              (0 to nrReply.asInstanceOf[Int]) foreach { i =>
                val event: JsonObject = events.getObject(i)

                val serManifest = event.getString("manifest")
                val serId = event.getInt("id")
                val bytes = Base64.getDecoder.decode(event.getString("payload"))
                val payload = serialization.deserialize(bytes, serId, serManifest).get // hrmm
                recoveryCallback(PersistentRepr(
                  payload = payload,
                  sequenceNr = from + i,
                  persistenceId = persistenceId,
                  writerUuid = writerUuid)
                )
              }
            }
          }
        )
      done.future
    }


    sayWhat
  }

  // TODO how horrific is this query?
  // select persistenceId, sequence_from from akka where akka.persistenceId = "pid1" order by sequence_from desc limit 1
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.info("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)

    val result = Promise[Long]

    val highestSequenceNrStatement = select("sequence_to")
      .from("akka")
      .where(x("akka.persistenceId").eq(x("$1"))
        .and(x("akka.sequence_from").gte(fromSequenceNr)))
      .orderBy(desc("akka.sequence_from"))
      .limit(1)


    // Without this the index that gets the latest sequence nr may have not seen the last write of the last version
    // of this persistenceId. This seems overkill.
    val params: N1qlParams = N1qlParams.build()
      .consistency(ScanConsistency.REQUEST_PLUS)

    val highestSequenceNrQuery = N1qlQuery.parameterized(highestSequenceNrStatement, JsonArray.from(persistenceId), params)

    log.info("Executing: {}", highestSequenceNrQuery)

    bucket.query(highestSequenceNrQuery)
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
