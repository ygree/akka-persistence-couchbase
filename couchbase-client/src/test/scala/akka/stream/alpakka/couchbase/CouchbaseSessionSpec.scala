/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import akka.stream.javadsl.Sink
import akka.testkit.TestKit
import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.{N1qlParams, N1qlQuery}
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

class CouchbaseSessionSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll with Eventually {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 150.millis)

  private implicit val system = ActorSystem("CouchbaseSessionSpec")
  private implicit val materializer = ActorMaterializer()
  import system.dispatcher

  private val bucketName = "couchbaseSessionTest"
  private val cluster: Cluster = {
    val c = CouchbaseCluster.create()
    c.authenticate("admin", "admin1") // needs to be admin
    val manager = c.clusterManager()

    // make sure each test run is from a clean slate
    if (manager.hasBucket(bucketName)) {
      manager.removeBucket(bucketName)
    }
    val bucketSettings = new DefaultBucketSettings.Builder()
      .`type`(BucketType.COUCHBASE)
      .name(bucketName)
      .quota(100)
      .build()
    manager.insertBucket(bucketSettings)

    c
  }
  private val bucket = cluster.openBucket(bucketName)
  bucket.bucketManager().createN1qlIndex("intvals", true, false, "intVal")

  // test CouchbaseSession via its Java adapter to test both in one shot
  val session: CouchbaseSession = CouchbaseSession.create(cluster.openBucket(bucketName))

  "The couchbase session" should {

    "allow for CRUD operations on documents" in {
      val insertObject = JsonObject.create()
      insertObject.put("intVal", 1)

      val inserted = session.insert(JsonDocument.create("one", insertObject)).toScala.futureValue
      inserted.id() should ===("one")
      inserted.content().getInt("intVal") should ===(1)

      val getResult = session.get("one").toScala.futureValue.asScala
      getResult should not be empty
      val getDoc: JsonDocument = getResult.get
      getDoc.id() should ===("one")
      getDoc.content().getInt("intVal") should ===(1)

      val upsertObject = JsonObject.create()
      upsertObject.put("intVal", 3)
      upsertObject.put("stringVal", "whoa")

      session.upsert(JsonDocument.create("one", upsertObject)).toScala.futureValue

      val query = select("*")
        .from(bucketName)
        .where(x("intVal").eq(3))

      val queryResult = eventually {
        val result = session.singleResponseQuery(query).toScala.futureValue.asScala
        result should not be empty
        result
      }

      // FIXME can we hide this nesting in query results or does it make sense to keep for some reason?
      val queryObject = queryResult.get.getObject(bucketName)
      queryObject.getInt("intVal") should be(3)
      queryObject.getString("stringVal") should be("whoa")

      session.remove("one").toScala.futureValue
    }

    "stream query results" in {

      bucket.bucketManager().createN1qlIndex("q", true, false, "daField")

      Future
        .traverse(1 to 1000) { n =>
          val obj = JsonObject.create()
          obj.put("daField", n)
          session.insert(JsonDocument.create(s"q-$n", obj)).toScala
        }
        .futureValue

      val queryParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)

      val statement = select("*")
        .from(bucketName)
        .where(x("daField").isValued)

      val query = N1qlQuery.simple(statement, queryParams)

      // FIXME verify backpressure somehow
      val queryResult = session.streamedQuery(query).runWith(Sink.seq, materializer).toScala.futureValue

      queryResult should have size 1000
    }

    "upsert a missing document" in {
      val upsertObject = JsonObject.create()
      upsertObject.put("intVal", 5)
      upsertObject.put("stringVal", "whoa")

      session.upsert(JsonDocument.create("upsert-missing", upsertObject)).toScala.futureValue
      val persisted = session.get("upsert-missing").toScala.futureValue.asScala
      persisted should not be empty
      persisted.get.content().getInt("intVal") should ===(5)
    }

    "allow for counters" in {
      val v1 = session.counter("c1", 1, 0).toScala.futureValue
      val v2 = session.counter("c1", 1, 0).toScala.futureValue
      v1 should ===(0L) // starts at 0
      v2 should ===(1L)
    }
  }

  override protected def afterAll(): Unit = {
    session.close().toScala.futureValue
    cluster.clusterManager().removeBucket(bucketName)
    cluster.disconnect()
    TestKit.shutdownActorSystem(system)
  }
}
