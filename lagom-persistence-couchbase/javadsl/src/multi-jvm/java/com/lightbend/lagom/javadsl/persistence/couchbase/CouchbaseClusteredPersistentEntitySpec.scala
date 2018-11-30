package com.lightbend.lagom.javadsl.persistence.couchbase

import java.util.concurrent.CompletionStage

import akka.persistence.couchbase.CouchbaseClusterConnection
import com.lightbend.lagom.internal.persistence.couchbase.TestConfig
import com.lightbend.lagom.internal.persistence.testkit.AwaitPersistenceInit.awaitPersistenceInit
import com.lightbend.lagom.javadsl.persistence.{ReadSideProcessor, TestEntityReadSide}
import com.lightbend.lagom.javadsl.persistence.multinode.{
  AbstractClusteredPersistentEntityConfig,
  AbstractClusteredPersistentEntitySpec
}
import com.lightbend.lagom.javadsl.persistence.TestEntity.Evt
import com.typesafe.config.Config
import play.api.inject.DefaultApplicationLifecycle

object CouchbaseClusteredPersistentEntityConfig extends AbstractClusteredPersistentEntityConfig {
  override def additionalCommonConfig(databasePort: Int): Config =
    TestConfig.persistenceConfig
}

class CouchbaseClusteredPersistentEntitySpecMultiJvmNode1 extends CouchbaseClusteredPersistentEntitySpec
class CouchbaseClusteredPersistentEntitySpecMultiJvmNode2 extends CouchbaseClusteredPersistentEntitySpec
class CouchbaseClusteredPersistentEntitySpecMultiJvmNode3 extends CouchbaseClusteredPersistentEntitySpec

class CouchbaseClusteredPersistentEntitySpec
    extends AbstractClusteredPersistentEntitySpec(CouchbaseClusteredPersistentEntityConfig) {

  import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbaseClusteredPersistentEntityConfig._

  override protected def atStartup(): Unit = {
    runOn(node1) {
      CouchbaseClusterConnection.connect().cleanUp().close()
      awaitPersistenceInit(system)
    }
    enterBarrier("couchbase-started")

    super.atStartup()
  }

  lazy val defaultApplicationLifecycle = new DefaultApplicationLifecycle

  def testEntityReadSide = injector.instanceOf[TestEntityReadSide]

  override protected def getAppendCount(id: String): CompletionStage[java.lang.Long] =
    testEntityReadSide.getAppendCount(id)

  override protected def readSideProcessor: Class[_ <: ReadSideProcessor[Evt]] =
    classOf[TestEntityReadSide.TestEntityReadSideProcessor]
}
