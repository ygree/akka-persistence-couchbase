/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import java.io.File

import akka.actor.ActorSystem
import akka.persistence.couchbase.CouchbaseClusterConnection
import akka.stream.{ActorMaterializer, Materializer}
import com.lightbend.lagom.internal.persistence.couchbase.TestConfig
import com.lightbend.lagom.internal.persistence.testkit.AwaitPersistenceInit.awaitPersistenceInit
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.persistence.multinode.{AbstractClusteredPersistentEntityConfig, AbstractClusteredPersistentEntitySpec}
import com.lightbend.lagom.scaladsl.persistence.{ReadSideProcessor, TestEntity}
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.typesafe.config.Config
import play.api.{Configuration, Environment, Mode}
import play.api.inject.DefaultApplicationLifecycle

import scala.concurrent.{ExecutionContext, Future}

object CouchbaseClusteredPersistentEntityConfig extends AbstractClusteredPersistentEntityConfig {
  override def additionalCommonConfig(databasePort: Int): Config =
    TestConfig.persistenceConfig
}

class CouchbaseClusteredPersistentEntitySpecMultiJvmNode1 extends CouchbaseClusteredPersistentEntitySpec
class CouchbaseClusteredPersistentEntitySpecMultiJvmNode2 extends CouchbaseClusteredPersistentEntitySpec
class CouchbaseClusteredPersistentEntitySpecMultiJvmNode3 extends CouchbaseClusteredPersistentEntitySpec

class CouchbaseClusteredPersistentEntitySpec
    extends AbstractClusteredPersistentEntitySpec(CouchbaseClusteredPersistentEntityConfig) {

  import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseClusteredPersistentEntityConfig._

  override protected def atStartup(): Unit = {
    runOn(node1) {
      CouchbaseClusterConnection.connect().cleanUp().close()
      awaitPersistenceInit(system)
    }
    enterBarrier("couchbase-started")

    super.atStartup()
  }

  lazy val defaultApplicationLifecycle = new DefaultApplicationLifecycle

  override lazy val components: CouchbasePersistenceComponents =
    new CouchbasePersistenceComponents {
      override def actorSystem: ActorSystem = system
      override def executionContext: ExecutionContext = system.dispatcher
      override def materializer: Materializer = ActorMaterializer()(system)
      override def configuration: Configuration = Configuration(system.settings.config)
      override def serviceLocator: ServiceLocator = NoServiceLocator
      override def environment: Environment = Environment(new File("."), getClass.getClassLoader, Mode.Test)
      override def jsonSerializerRegistry: JsonSerializerRegistry = ???
    }

  def testEntityReadSide = new TestEntityReadSide(components.actorSystem, components.couchbase)

  override protected def readSideProcessor: () => ReadSideProcessor[TestEntity.Evt] =
    () => new TestEntityReadSide.TestEntityReadSideProcessor(system, components.couchbaseReadSide)

  override protected def getAppendCount(id: String): Future[Long] = testEntityReadSide.getAppendCount(id)
}
