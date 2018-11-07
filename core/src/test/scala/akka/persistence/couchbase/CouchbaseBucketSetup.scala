/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import com.couchbase.client.java.query.N1qlQuery
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Try

trait CouchbaseBucketSetup extends BeforeAndAfterAll { self: Suite =>

  var session: CouchbaseSession = _
  var cluster: Cluster = _

  override protected def beforeAll(): Unit = {

    val bucketName = "akka"
    cluster = CouchbaseCluster
      .create()
      .authenticate("admin", "admin1") // needs to be admin

    val bucket = cluster.openBucket(bucketName)

    val result = bucket.query(N1qlQuery.simple("delete from akka"), 5, TimeUnit.MINUTES)
    assert(result.finalSuccess())

    session = CouchbaseSession(bucket)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    Try(session.close())
    Try(cluster.disconnect())
    super.afterAll()
  }

}
