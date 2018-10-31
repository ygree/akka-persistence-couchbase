package akka.persistence.couchbase

import akka.persistence.snapshot.SnapshotStoreSpec
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.config.ConfigFactory

class CouchbaseSnapshotStoreSpec extends SnapshotStoreSpec(ConfigFactory.load()) {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    CouchbaseCluster.create()
      .authenticate("admin", "admin1")
      .openBucket("akka")
      .query(N1qlQuery.simple("delete from akka"))
  }
}
