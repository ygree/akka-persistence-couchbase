/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import akka.testkit.WithLogCapturing
import com.typesafe.config.ConfigFactory
import org.scalatest.Ignore

import scala.concurrent.duration._

// this test is quite heavy and we don't want to run it on travis
// remove annotation to run locally
@Ignore
class CouchbaseJournalPerfSpec
    extends JournalPerfSpec(ConfigFactory.parseString("""
          akka.loglevel = debug
          akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
        """).withFallback(ConfigFactory.load()))
    with CouchbaseBucketSetup
    with WithLogCapturing {

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  // We want to test with persisting guaranteed, which makes
  // it quite slow. This was adjusted to pass on travis.
  override def eventsCount: Int = 1000

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false
}
