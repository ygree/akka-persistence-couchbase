/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.{ Cluster, CouchbaseAsyncBucket, CouchbaseBucket, CouchbaseCluster }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers, WordSpec }

import scala.concurrent.duration._

class CouchbaseSessionSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds)

  private implicit val system = ActorSystem("CouchbaseSessionSpec")
  private implicit val materializer = ActorMaterializer()

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
  val session = new CouchbaseSession(cluster.openBucket(bucketName))

  "The couchbase session" should {

    "allow for CRUD operations on documents" in {
      val insertObject = JsonObject.create()
      insertObject.put("intVal", 1)

      session.insert(
        JsonDocument.create(
          "one",
          insertObject)).futureValue

      val getResult = session.get("one").futureValue
      getResult should not be empty
      val getDoc: JsonDocument = getResult.get
      getDoc.id() should ===("one")
      getDoc.content().getInt("intVal") should ===(1)

      val upsertObject = JsonObject.create()
      upsertObject.put("intVal", 3)
      upsertObject.put("stringVal", "whoa")

      session.upsert(JsonDocument.create("one", upsertObject)).futureValue

      val query = select("*")
        .from(bucketName)
        .where(x("intVal").eq(3))
      val queryResult = session.singleResponseQuery(query).futureValue

      queryResult should not be empty

      // FIXME can we hide this nesting in query results or does it make sense to keep for some reason?
      val queryObject = queryResult.get.getObject(bucketName)
      queryObject.getInt("intVal") should be(3)
      queryObject.getString("stringVal") should be("whoa")
    }

    "allow for counters" in {
      val v1 = session.counter("c1", 1, 0).futureValue
      val v2 = session.counter("c1", 1, 0).futureValue
      v1.content() should ===(0L) // starts at 0
      v2.content() should ===(1L)
    }
  }

  override protected def afterAll(): Unit = {
    session.close().futureValue
    cluster.disconnect()
    TestKit.shutdownActorSystem(system)
  }
}
