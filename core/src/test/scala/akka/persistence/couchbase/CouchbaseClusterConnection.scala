/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import com.couchbase.client.java.query.N1qlQuery

import scala.concurrent.Await
import scala.util.Try
import scala.concurrent.duration._

object CouchbaseClusterConnection {

  def connect(): CouchbaseClusterConnection = connect("admin", "admin1", "akka")

  def connect(username: String, password: String, bucketName: String): CouchbaseClusterConnection = {

    val cluster = CouchbaseCluster.create()
    cluster.authenticate(username, password) // needs to be admin

    val bucket = cluster.openBucket(bucketName)

    new CouchbaseClusterConnection(cluster, bucket)
  }

}

final class CouchbaseClusterConnection(val cluster: Cluster, bucket: Bucket) {

  val couchbaseSession: CouchbaseSession = CouchbaseSession(bucket)

  def cleanUp(): CouchbaseClusterConnection = {
    val bucketName = bucket.name()
    bucket.bucketManager().createN1qlPrimaryIndex(true, false)
    val result = bucket.query(N1qlQuery.simple(s"delete from $bucketName"), 5, TimeUnit.MINUTES)
    assert(result.finalSuccess(), s"Failed to clean out bucket $bucketName")
    bucket.bucketManager().dropN1qlPrimaryIndex(true)

    this
  }

  def close(): Unit = {
    Try(Await.result(couchbaseSession.close(), 30.seconds))
    Try(cluster.disconnect())
  }
}
