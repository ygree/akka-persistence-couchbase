/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

class CouchbaseSnapshotStoreSpec
    extends SnapshotStoreSpec(
      ConfigFactory
        .parseString(
          """
            |akka.persistence.snapshot-store.plugin = "couchbase-journal.snapshot"
          """.stripMargin
        )
        .withFallback(ConfigFactory.load())
    )
    with CouchbaseBucketSetup {}
