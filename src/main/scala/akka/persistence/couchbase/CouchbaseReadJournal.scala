package akka.persistence.couchbase

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.couchbase.internal.CouchbaseQueryStage
import akka.persistence.query.scaladsl._
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.config.Config
import rx.{Observable, RxReactiveStreams}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.dsl.functions._
import com.couchbase.client.java.query.dsl.functions.AggregateFunctions._
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import org.reactivestreams.Publisher

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

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    // I am currently working out the best type of index for this
    ???
  }

  /**
    * select  distinct persistenceId from akka where persistenceId is not null
    */
  override def currentPersistenceIds(): Source[String, NotUsed] = {

    // this type works on the current queries we'd need to create a stage
    // to do the live queries

    // this can fail as it relies on async updates to the index.
    val query = select(distinct("persistenceId")).from("akka").where(x("persistenceId").isNotNull)
    val rows: Observable[AsyncN1qlQueryRow] = bucket.query(query).flatMap(results => results.rows())
    // FIXME, deal with initial query failure .errors()
    val publisher: Publisher[AsyncN1qlQueryRow] = RxReactiveStreams.toPublisher(rows)
    val source: Source[AsyncN1qlQueryRow, NotUsed] = Source.fromPublisher(publisher)

    source.map(row => row.value().getString("persistenceId"))
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
