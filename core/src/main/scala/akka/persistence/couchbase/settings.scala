/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import com.typesafe.config.Config

final class CouchbaseJournalSettings private (val sessionSettings: CouchbaseSessionSettings, val bucket: String)

object CouchbaseJournalSettings {

  def apply(config: Config): CouchbaseJournalSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)

    new CouchbaseJournalSettings(sessionSettings, bucket)
  }
}

final class CouchbaseReadJournalSettings private (val sessionSettings: CouchbaseSessionSettings, val bucket: String)

object CouchbaseReadJournalSettings {

  def apply(config: Config): CouchbaseReadJournalSettings = {
    // FIXME uses the same config as CouchbaseJournalSettings for now
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)

    new CouchbaseReadJournalSettings(sessionSettings, bucket)
  }
}

final class CouchbaseSnapshotSettings private (val sessionSettings: CouchbaseSessionSettings, val bucket: String)

object CouchbaseSnapshotSettings {

  def apply(config: Config): CouchbaseSnapshotSettings = {
    // FIXME uses the same config as CouchbaseJournalSettings for now
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("snapshot.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)

    new CouchbaseSnapshotSettings(sessionSettings, bucket)
  }
}
