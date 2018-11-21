/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

// FIXME this is currently almost test-kit used across all modules, make it testkit or duplicate instead of a test-test dependency?
trait CouchbaseBucketSetup extends BeforeAndAfterAll { self: Suite =>

  private var cluster: Cluster = _
  protected var couchbaseSession: CouchbaseSession = _
  // FIXME pick these up from config instead
  val bucketName = "akka"

  override protected def beforeAll(): Unit = {

    cluster = CouchbaseCluster.create()
    cluster.authenticate("admin", "admin1") // needs to be admin

    val bucket = cluster.openBucket(bucketName)

    bucket.bucketManager().createN1qlPrimaryIndex(true, false)
    val result = bucket.query(N1qlQuery.simple(s"delete from $bucketName"), 5, TimeUnit.MINUTES)
    assert(result.finalSuccess(), s"Failed to clean out bucket $bucketName")
    bucket.bucketManager().dropN1qlPrimaryIndex(true)

    couchbaseSession = CouchbaseSession(bucket)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    Try(Await.result(couchbaseSession.close(), 30.seconds))
    Try(cluster.disconnect())
    super.afterAll()
  }

}
