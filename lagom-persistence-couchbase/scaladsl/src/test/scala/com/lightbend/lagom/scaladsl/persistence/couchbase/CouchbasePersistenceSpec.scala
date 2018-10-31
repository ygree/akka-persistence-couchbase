/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import java.io.File

import akka.actor.setup.ActorSystemSetup
import akka.actor.{ActorSystem, BootstrapSetup}
import akka.cluster.Cluster
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import com.lightbend.lagom.internal.persistence.testkit.AwaitPersistenceInit.awaitPersistenceInit
import com.lightbend.lagom.persistence.PersistenceSpec
import com.lightbend.lagom.persistence.ActorSystemSpec
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.typesafe.config.{Config, ConfigFactory}
import com.lightbend.lagom.internal.persistence.testkit.PersistenceTestConfig._


object CouchbasePersistenceSpec {

  val couchbaseConfigMap: Map[String, AnyRef] = Map(
    "akka.persistence.journal.plugin" -> "akka.persistence.couchbase.journal",
    "akka.persistence.snapshot-store.plugin" -> "akka.persistence.couchbase.snapshot",
  )

  import scala.collection.JavaConverters._

  def couchbaseConfig(): Config = ConfigFactory.parseMap(couchbaseConfigMap.asJava)

}

class CouchbasePersistenceSpec private(system: ActorSystem) extends ActorSystemSpec(system) {

  import CouchbasePersistenceSpec._


  def this(testName: String, config: Config, jsonSerializerRegistry: JsonSerializerRegistry) =
    this(ActorSystem(testName, ActorSystemSetup(
      BootstrapSetup(
        config
          .withFallback(CouchbasePersistenceSpec.couchbaseConfig())
          .withFallback(ClusterConfig)
      ),
      JsonSerializerRegistry.serializationSetupFor(jsonSerializerRegistry)
    )))

  def this(config: Config, jsonSerializerRegistry: JsonSerializerRegistry) = this(PersistenceSpec.getCallerName(getClass), config, jsonSerializerRegistry)

  def this(jsonSerializerRegistry: JsonSerializerRegistry) = this(ConfigFactory.empty(), jsonSerializerRegistry)

  override def beforeAll(): Unit = {
    CouchbaseCluster.create()
      .authenticate("admin", "admin1")
      .openBucket("akka")
      .query(N1qlQuery.simple("delete from akka"))

    super.beforeAll()
    awaitPersistenceInit(system)

    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

}
