/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.config.ConfigFactory

class CouchbaseJournalSpec extends JournalSpec(ConfigFactory.load()) with CouchbaseBucketSetup {

  override def supportsRejectingNonSerializableObjects: CapabilityFlag =
    false // or CapabilityFlag.off

  override def supportsSerialization: CapabilityFlag =
    true // or CapabilityFlag.on
}
