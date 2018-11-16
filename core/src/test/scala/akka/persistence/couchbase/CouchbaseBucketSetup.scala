/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.dispatch.ExecutionContexts
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Try

trait CouchbaseBucketSetup extends BeforeAndAfterAll { self: Suite =>

  private var cluster: Cluster = _
  var couchbase: Couchbase = _

  override protected def beforeAll(): Unit = {

    val bucketName = "akka"
    cluster = CouchbaseCluster.create()
    cluster.authenticate("admin", "admin1") // needs to be admin

    val bucket = cluster.openBucket(bucketName)

    bucket.bucketManager().createN1qlPrimaryIndex(true, false)

    val result = bucket.query(N1qlQuery.simple("delete from akka"), 5, TimeUnit.MINUTES)
    assert(result.finalSuccess())

    bucket.bucketManager().dropN1qlPrimaryIndex(true)

    couchbase = Couchbase(bucket)(ExecutionContexts.sameThreadExecutionContext)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    Try(couchbase.close())
    Try(cluster.disconnect())
    super.afterAll()
  }

}
