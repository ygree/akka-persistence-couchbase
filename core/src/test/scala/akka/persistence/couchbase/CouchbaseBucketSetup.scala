/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.query.Index
import com.couchbase.client.java.query.dsl.clause._
import com.couchbase.client.java.query.dsl.Expression._
import org.scalatest.{BeforeAndAfterAll, Suite}

trait CouchbaseBucketSetup extends BeforeAndAfterAll { self: Suite =>

  var session: CouchbaseSession = _

  override protected def beforeAll(): Unit = {
    val bucketName = "akka"
    val cluster: Cluster = {
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
    val bucket = cluster.openBucket(bucketName)
    bucket.bucketManager().createN1qlIndex("pi2", true, false, "persistence_id", "sequence_from")
    // FIXME figure out how to create that tags index
    // bucket.bucketManager().createN1qlIndex("tags", true, false, Index.createIndex("tags").on(bucketName, x(""), x("ordering"))
    session = CouchbaseSession(bucket)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    // FIXME drop bucket as well?
    session.close()
    super.afterAll()
  }

}
