package akka.persistence.couchbase

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.couchbase.CouchbaseJournal.{Fields, TaggedPersistentRepr, deserialize, extractTaggedEvent}
import akka.persistence.query.scaladsl._
import akka.persistence.query._
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.functions.AggregateFunctions._
import com.typesafe.config.Config
import rx.{Observable, RxReactiveStreams}

import scala.collection.JavaConverters._
import scala.collection.immutable

object CouchbaseReadJournal {
  final val Identifier = "akka.persistence.couchbase.query"
}

/*
Required indexes:

CREATE INDEX `pi2` ON `akka`((self.`persistenceId`),(self.`sequence_from`))


 */
class CouchbaseReadJournal(as: ExtendedActorSystem, config: Config, configPath: String) extends ReadJournal
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery
  with CurrentPersistenceIdsQuery
  with PersistenceIdsQuery {


  private val serialization: Serialization = SerializationExtension(as)
  // TODO config
  private val settings = CouchbaseSettings(config)
  // FIXME, hosts from config
  private val cluster = CouchbaseCluster.create().authenticate(settings.username, settings.password)
  private val bucket = cluster.openBucket(settings.bucket).async()

  as.registerOnTermination {
    cluster.disconnect()
  }

  private val eventsByTagQuery =
    """select * FROM akka
      |WHERE ANY tag IN akka.all_tags SATISFIES tag = $tag END
      |AND ordering >= $ordering
      |ORDER BY ordering""".stripMargin

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    val initialOrdering: Long = offset match {
      case NoOffset => 0L
      case Sequence(o) => o
      case TimeBasedUUID(_) => throw new IllegalArgumentException("Couchbase Journal does not support Timeuuid offsets")
    }

    val params: JsonObject = JsonObject.create().put("tag", tag)
    val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)

    eventsByTagSource(Source.fromGraph(new N1qlQueryStage[Long](
      N1qlQuery.parameterized(eventsByTagQuery, params.put(Fields.Ordering, initialOrdering), queryParams),
      params, bucket, initialOrdering,
      ordering => Some(N1qlQuery.parameterized(eventsByTagQuery, params.put(Fields.Ordering, ordering), queryParams)),
      (_, row) => row.value().getObject("akka").getLong(Fields.Ordering) + 1
    )).mapMaterializedValue(_ => NotUsed), tag)
  }

  private val eventsByPersistenceId =
    """
      |select * from akka
      |where persistence_id = $pid
      |and sequence_from  >= $from
      |and sequence_from <= $to
      |order by sequence_from
    """.stripMargin


  case class EventsByPersistenceIdState(from: Long, to: Long)

  // FIXME, filter out messages based on toSerquenceNr when they have been saved into a batch
  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    val params: JsonObject = JsonObject.create()
      .put("pid", persistenceId)
      .put("to", toSequenceNr)

    val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)
    // TODO do the deleted to query first and start from higher of that and fromSequenceNr
    val source = Source.fromGraph(new N1qlQueryStage[EventsByPersistenceIdState](
      N1qlQuery.parameterized(eventsByPersistenceId, params.put("from", fromSequenceNr), queryParams),
      params,
      bucket,
      EventsByPersistenceIdState(fromSequenceNr, 0),
      state => {
        if (state.to >= toSequenceNr)
          None
        else
          Some(N1qlQuery.parameterized(eventsByPersistenceId, params.put("from", state.from), queryParams))
      },
      (_, row) => EventsByPersistenceIdState(
        row.value().getObject("akka").getLong(Fields.SequenceFrom) + 1,
        row.value().getObject("akka").getLong(Fields.SequenceTo)
      )
    )).mapMaterializedValue(_ => NotUsed)

    eventsByPersistenceIdSource(source)
  }

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    val params: JsonObject = JsonObject.create()
      .put("pid", persistenceId)
      .put("to", toSequenceNr)

    val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)
    // TODO do the deleted to query first and start from higher of that and fromSequenceNr

    val source = Source.fromGraph(new N1qlQueryStage[EventsByPersistenceIdState](
      N1qlQuery.parameterized(eventsByPersistenceId, params.put("from", fromSequenceNr), queryParams),
      params,
      bucket,
      EventsByPersistenceIdState(fromSequenceNr, 0),
      state => None,
      (_, row) => EventsByPersistenceIdState(
        row.value().getObject("akka").getLong(Fields.SequenceFrom) + 1,
        row.value().getObject("akka").getLong(Fields.SequenceTo)
      )
    )).mapMaterializedValue(_ => NotUsed)

    eventsByPersistenceIdSource(source)
  }

  /*
  Messages persisted together with PersistAll have the same Offset

   CREATE INDEX `tags` ON `akka`((all (`all_tags`)),`ordering`)
   */
  // TODO, just the stage now it allows the query to stop
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    // TODO make configurable
    val params = N1qlParams.build()
      //      .consistency(ScanConsistency.NOT_BOUNDED) // async
      .consistency(ScanConsistency.REQUEST_PLUS) // read own writes

    val query = offset match {
      case NoOffset =>
        N1qlQuery.parameterized(
          """select * FROM akka
            |WHERE ANY tag IN akka.all_tags SATISFIES tag = $1 END
            |ORDER BY ordering""".stripMargin, JsonArray.from(tag), params)

      case Sequence(o: Long) =>
        N1qlQuery.parameterized(
          """select * FROM akka
            |WHERE ANY tag IN akka.all_tags SATISFIES tag = $1 END
            |AND ordering >= $2
            |ORDER BY ordering""".stripMargin, JsonArray.from(List(o, tag).asJava), params)

      case TimeBasedUUID(_) =>
        throw new IllegalArgumentException("TimeBasedUUIDs are not supported")
    }

    eventsByTagSource(n1qlQuery(query), tag)
  }

  private def eventsByPersistenceIdSource(in: Source[AsyncN1qlQueryRow, NotUsed]): Source[EventEnvelope, NotUsed] = {
    in.map((row: AsyncN1qlQueryRow) => {
      val value = row.value().getObject("akka")
      val deserialized: immutable.Seq[TaggedPersistentRepr] = deserialize(value, Long.MaxValue, serialization, extractTaggedEvent)
      deserialized.map(tpr => EventEnvelope(
        Offset.sequence(tpr.pr.sequenceNr), // FIXME, should this be +1, check inclusivity of offsets
        tpr.pr.persistenceId,
        tpr.pr.sequenceNr,
        tpr.pr.payload
      ))
    }).mapConcat(identity)
  }

  private def eventsByTagSource(in: Source[AsyncN1qlQueryRow, NotUsed], tag: String): Source[EventEnvelope, NotUsed] = {
    in.map((row: AsyncN1qlQueryRow) => {
      val value = row.value().getObject("akka")
      val deserialized: immutable.Seq[TaggedPersistentRepr] = deserialize(value, Long.MaxValue, serialization, extractTaggedEvent).filter(_.tags.contains(tag))
      val ordering = value.getLong(Fields.Ordering)
      deserialized.map(tpr => EventEnvelope(
        Offset.sequence(ordering + 1), // set to the next one so resume doesn't get this event back
        tpr.pr.persistenceId,
        tpr.pr.sequenceNr,
        tpr.pr.payload
      ))
    }).mapConcat(identity)
  }

  /**
    * select  distinct persistenceId from akka where persistenceId is not null
    */
  override def currentPersistenceIds(): Source[String, NotUsed] = {

    // this type works on the current queries we'd need to create a stage
    // to do the live queries
    // this can fail as it relies on async updates to the index.
    val query = select(distinct(Fields.PersistenceId)).from("akka").where(x(Fields.PersistenceId).isNotNull)
    n1qlQuery(query).map(row => row.value().getString(Fields.PersistenceId))
  }

  private def n1qlQuery(query: N1qlQuery): Source[AsyncN1qlQueryRow, NotUsed] = {
    Source.fromPublisher(RxReactiveStreams.toPublisher({
      bucket.query(query).flatMap(results => results.rows())
    }))
  }
  private def n1qlQuery(query: Statement): Source[AsyncN1qlQueryRow, NotUsed] = {
    Source.fromPublisher(RxReactiveStreams.toPublisher(
      bucket.query(query).flatMap(results => results.rows()))
    )
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
  override def persistenceIds(): Source[String, NotUsed] = {
    ???
  }
}
