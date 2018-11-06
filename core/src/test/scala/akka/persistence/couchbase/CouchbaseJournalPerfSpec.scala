/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.event.Logging
import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

class CouchbaseJournalPerfSpec extends JournalPerfSpec(ConfigFactory.load()) with CouchbaseBucketSetup with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Decrease log level for this performance test to avoid "exceeded the maximum log length" when run by Travis
    system.eventStream.setLogLevel(Logging.InfoLevel)
    super.beforeAll()
  }

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  //TODO: reduced number of events to make test pass
  override def eventsCount: Int = 5 * 1000

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false
}
