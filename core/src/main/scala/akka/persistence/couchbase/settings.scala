/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, CouchbaseWriteSettings}
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.Config
import scala.concurrent.duration._

final class CouchbaseJournalSettings private (val sessionSettings: CouchbaseSessionSettings,
                                              val bucket: String,
                                              val writeSettings: CouchbaseWriteSettings,
                                              val readTimeout: FiniteDuration,
                                              val indexAutoCreate: Boolean)

object CouchbaseJournalSettings {

  def apply(config: Config): CouchbaseJournalSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)
    val writeSettings = CouchbaseWriteSettings(
      parallelism = config.getInt("write.parallelism"),
      replicateTo = parseReplicateTo(config.getString("write.replicate-to")),
      persistTo = parsePersistTo(config.getString("write.persist-to")),
      timeout = config.getDuration("write.write-timeout").toMillis.millis
    )
    val readTimeout = config.getDuration("write.read-timeout").toMillis.millis
    val indexAutoCreate = config.getBoolean("write.index-autocreate")

    new CouchbaseJournalSettings(sessionSettings, bucket, writeSettings, readTimeout, indexAutoCreate)
  }

  private def parseReplicateTo(value: String): ReplicateTo = value match {
    case "none" => ReplicateTo.NONE
    case "one" => ReplicateTo.ONE
    case "two" => ReplicateTo.TWO
    case "three" => ReplicateTo.THREE
    case unknown => throw new IllegalArgumentException(s"Unknown replicate-to value: $unknown")
  }

  private def parsePersistTo(value: String): PersistTo = value match {
    case "none" => PersistTo.NONE
    case "master" => PersistTo.MASTER
    case "one" => PersistTo.ONE
    case "two" => PersistTo.TWO
    case "three" => PersistTo.THREE
    case "four" => PersistTo.FOUR
    case unknown => throw new IllegalArgumentException(s"Unknown persist-to value: $unknown")
  }
}

final class CouchbaseReadJournalSettings private (val sessionSettings: CouchbaseSessionSettings,
                                                  val bucket: String,
                                                  val indexAutoCreate: Boolean)

object CouchbaseReadJournalSettings {

  def apply(config: Config): CouchbaseReadJournalSettings = {
    // FIXME uses the same config as CouchbaseJournalSettings for now
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)
    val indexAutoCreate = config.getBoolean("read.index-autocreate")

    new CouchbaseReadJournalSettings(sessionSettings, bucket, indexAutoCreate)
  }
}

final class CouchbaseSnapshotSettings private (val sessionSettings: CouchbaseSessionSettings,
                                               val bucket: String,
                                               val indexAutoCreate: Boolean)

object CouchbaseSnapshotSettings {

  def apply(config: Config): CouchbaseSnapshotSettings = {
    // FIXME uses the same config as CouchbaseJournalSettings for now
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("snapshot.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)
    val indexAutoCreate = config.getBoolean("snapshot.index-autocreate")

    new CouchbaseSnapshotSettings(sessionSettings, bucket, indexAutoCreate)
  }
}
