/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.query.{Index, N1qlQuery}
import com.couchbase.client.java.query.dsl.clause._
import com.couchbase.client.java.query.dsl.Expression._
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

trait CouchbaseBucketSetup extends BeforeAndAfterAll { self: Suite =>

  private var cluster: Cluster = _
  var session: CouchbaseSession = _

  override protected def beforeAll(): Unit = {

    val bucketName = "akka"
    cluster = CouchbaseCluster.create()
    cluster.authenticate("admin", "admin1") // needs to be admin
    val manager = cluster.clusterManager()

    val bucket = cluster.openBucket(bucketName)

    bucket.bucketManager().createN1qlPrimaryIndex(true, false)

    val result = bucket.query(N1qlQuery.simple("delete from akka"), 5, TimeUnit.MINUTES)
    assert(result.finalSuccess())

    bucket.bucketManager().dropN1qlPrimaryIndex(true)

    session = CouchbaseSession(bucket)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    Try(session.close())
    Try(cluster.disconnect())
    super.afterAll()
  }

}
