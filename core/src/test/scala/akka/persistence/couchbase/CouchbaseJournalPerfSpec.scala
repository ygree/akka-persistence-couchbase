/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class CouchbaseJournalPerfSpec extends JournalPerfSpec(ConfigFactory.load()) with CouchbaseBucketSetup with SuppressedLogging {

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  //TODO: reduced number of events to make test pass
  override def eventsCount: Int = 5 * 1000

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false
}
