package akka.persistence.couchbase

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

class CouchbaseJournalPerfSpec extends JournalPerfSpec(ConfigFactory.load()) {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    CouchbaseCluster.create()
      .authenticate("admin", "admin1")
      .openBucket("akka")
      .query(N1qlQuery.simple("delete from akka"))
  }

  override def awaitDurationMillis: Long = 40.seconds.toMillis

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false
}
