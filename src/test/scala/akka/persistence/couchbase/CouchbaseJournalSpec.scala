package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.config.ConfigFactory

class CouchbaseJournalSpec extends JournalSpec(ConfigFactory.load()) {

  protected override def beforeAll(): Unit = {
    // FIXME, use a global connection / get it from the journal
    super.beforeAll()
    CouchbaseCluster.create()
      .authenticate("admin", "admin1")
      .openBucket("akka")
      .query(N1qlQuery.simple("delete from akka"))
  }

  override def supportsRejectingNonSerializableObjects: CapabilityFlag =
    false// or CapabilityFlag.off

  override def supportsSerialization: CapabilityFlag =
    true // or CapabilityFlag.on
}
