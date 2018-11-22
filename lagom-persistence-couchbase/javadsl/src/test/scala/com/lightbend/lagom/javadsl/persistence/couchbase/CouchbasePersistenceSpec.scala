/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.persistence.couchbase.CouchbaseBucketSetup
import com.lightbend.lagom.internal.persistence.couchbase.TestConfig
import com.lightbend.lagom.internal.persistence.testkit.AwaitPersistenceInit.awaitPersistenceInit
import com.lightbend.lagom.internal.persistence.testkit.PersistenceTestConfig._
import com.lightbend.lagom.persistence.{ActorSystemSpec, PersistenceSpec}
import com.typesafe.config.{Config, ConfigFactory}

class CouchbasePersistenceSpec private (system: ActorSystem) extends ActorSystemSpec(system) with CouchbaseBucketSetup {

  def this(testName: String, config: Config) =
    this(
      ActorSystem(
        testName,
        config
          .withFallback(ClusterConfig)
          .withFallback(TestConfig.persistenceConfig())
      )
    )

  def this(config: Config) =
    this(PersistenceSpec.getCallerName(getClass), config)

  def this() = this(ConfigFactory.empty())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    awaitPersistenceInit(system)

    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
  }

}
