/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

class CouchbaseSnapshotStoreSpec extends SnapshotStoreSpec(ConfigFactory.load()) with CouchbaseBucketSetup {}
