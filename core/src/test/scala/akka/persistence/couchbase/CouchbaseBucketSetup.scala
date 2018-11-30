/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import org.scalatest.{BeforeAndAfterAll, Suite}

// FIXME this is currently almost test-kit used across all modules, make it testkit or duplicate instead of a test-test dependency?
trait CouchbaseBucketSetup extends BeforeAndAfterAll { self: Suite =>

  private var clusterConnection: CouchbaseClusterConnection = _
  protected def couchbaseSession: CouchbaseSession = clusterConnection.couchbaseSession

  override protected def beforeAll(): Unit = {
    clusterConnection = CouchbaseClusterConnection.connect().cleanUp()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    clusterConnection.close()
    super.afterAll()
  }

}
