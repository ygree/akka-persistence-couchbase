/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

class CouchbaseJournalPerfSpec extends JournalPerfSpec(ConfigFactory.load()) with CouchbaseBucketSetup {

  override def awaitDurationMillis: Long = 40.seconds.toMillis

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false
}
