package akka.persistence.couchbase

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.couchbase.CouchbaseJournal.{Fields, TaggedPersistentRepr, deserialize, extractTaggedEvent}
import akka.persistence.query.scaladsl._
import akka.persistence.query._
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.functions.AggregateFunctions._
import com.couchbase.client.java.query.dsl.path.GroupByPath
import com.typesafe.config.Config
import org.reactivestreams.Publisher
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

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = ???
  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    ???
  }

  /*
  Messages persisted together with PersistAll have the same Offset

   CREATE INDEX `tags` ON `akka`((all (`all_tags`)),`ordering`)
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    // TODO, include offset and workout index
    val query = offset match {
      case NoOffset =>
        N1qlQuery.parameterized(
          """select * FROM akka
            |WHERE ANY tag IN akka.all_tags SATISFIES tag = $1 END
            |ORDER BY ordering""".stripMargin, JsonArray.from(tag))

      case Sequence(o) =>
        N1qlQuery.parameterized(
          """select * FROM akka
            |WHERE ANY tag IN akka.all_tags SATISFIES tag = $1 END
            |AND ordering >= $2
            |ORDER BY ordering""".stripMargin, JsonArray.from(o, tag))

      case TimeBasedUUID(_) =>
        throw new IllegalArgumentException("TimeBasedUUIDs are not supported")
    }

    n1qlQuery(query).map((row: AsyncN1qlQueryRow) => {
      val deserialized: immutable.Seq[TaggedPersistentRepr] = deserialize(row, Long.MaxValue, serialization, extractTaggedEvent).filter(_.tags.contains(tag))
      val ordering = row.value().getLong(Fields.Ordering)
      deserialized.map(tpr => EventEnvelope(
        Offset.sequence(ordering), // FIXME, this won't work as
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
    Source.fromPublisher(RxReactiveStreams.toPublisher(
      bucket.query(query).flatMap(results => results.rows()))
    )
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
