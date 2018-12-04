/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class CouchbaseJournalSpec
    extends JournalSpec(
      ConfigFactory.parseString("""
    # JournalSpec uses small numbers of events (3-5), so make sure that page size is
    # smaller than that to cover paging
    couchbase-journal.write.replay-page-size=3
  """).withFallback(ConfigFactory.load())
    )
    with CouchbaseBucketSetup {

  override def supportsRejectingNonSerializableObjects: CapabilityFlag =
    false // or CapabilityFlag.off

  override def supportsSerialization: CapabilityFlag =
    true // or CapabilityFlag.on
}
