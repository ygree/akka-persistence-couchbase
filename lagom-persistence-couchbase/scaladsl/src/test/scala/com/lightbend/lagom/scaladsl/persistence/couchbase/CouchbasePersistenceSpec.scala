/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import akka.actor.setup.ActorSystemSetup
import akka.actor.{ActorSystem, BootstrapSetup}
import akka.cluster.Cluster
import akka.persistence.couchbase.CouchbaseBucketSetup
import com.lightbend.lagom.internal.persistence.testkit.AwaitPersistenceInit.awaitPersistenceInit
import com.lightbend.lagom.internal.persistence.testkit.PersistenceTestConfig._
import com.lightbend.lagom.persistence.{ActorSystemSpec, PersistenceSpec}
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.typesafe.config.{Config, ConfigFactory}

object CouchbasePersistenceSpec {

  def couchbaseConfig(): Config =
    ConfigFactory.parseString("""
      |akka.persistence.journal.plugin = "couchbase-journal.write"
      |akka.persistence.snapshot-store.plugin = "couchbase-journal.snapshot"
      |
      |couchbase-journal {
      |  connection {
      |    # nodes = [] # default
      |    username = "admin"
      |    password = "admin1"
      |  }
      |  write.bucket = "akka"
      |  write.index-autocreate=on
      |  read.index-autocreate=on
      |  snapshot.bucket = "akka"
      |}
    """.stripMargin)

}

class CouchbasePersistenceSpec private (system: ActorSystem) extends ActorSystemSpec(system) with CouchbaseBucketSetup {

  def this(testName: String, config: Config, jsonSerializerRegistry: JsonSerializerRegistry) =
    this(
      ActorSystem(
        testName,
        ActorSystemSetup(
          BootstrapSetup(
            config
              .withFallback(CouchbasePersistenceSpec.couchbaseConfig())
              .withFallback(ClusterConfig)
          ),
          JsonSerializerRegistry.serializationSetupFor(jsonSerializerRegistry)
        )
      )
    )

  def this(config: Config, jsonSerializerRegistry: JsonSerializerRegistry) =
    this(PersistenceSpec.getCallerName(getClass), config, jsonSerializerRegistry)

  def this(jsonSerializerRegistry: JsonSerializerRegistry) = this(ConfigFactory.empty(), jsonSerializerRegistry)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    awaitPersistenceInit(system)

    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
  }

}
