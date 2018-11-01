/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.{ AsyncBucket, Cluster, CouchbaseCluster }
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.scaladsl.persistence.couchbase.{ CouchbasePersistentEntityRegistry, CouchbaseReadSideImpl, ScaladslCouchbaseOffsetStore }
import com.lightbend.lagom.scaladsl.persistence.TestEntity.Evt
import com.lightbend.lagom.scaladsl.persistence._
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Future
import scala.concurrent.duration._

object CouchbaseReadSideSpec {

  val defaultConfig: Config = ConfigFactory.parseString("akka.loglevel = INFO")
}

class CouchbaseReadSideSpec extends CouchbasePersistenceSpec(CouchbaseReadSideSpec.defaultConfig, TestEntitySerializerRegistry) with AbstractReadSideSpec {
  import system.dispatcher

  override protected lazy val persistentEntityRegistry = new CouchbasePersistentEntityRegistry(system)

  private lazy val testSession: AsyncBucket = {
    CouchbaseCluster.create().authenticate("admin", "admin1").openBucket("akka").async()
  }
  private lazy val offsetStore = new ScaladslCouchbaseOffsetStore(system, testSession, ReadSideConfig())
  private lazy val couchbaseReadSide = new CouchbaseReadSideImpl(system, testSession, offsetStore)

  override def processorFactory(): ReadSideProcessor[Evt] =
    new TestEntityReadSide.TestEntityReadSideProcessor(system, couchbaseReadSide, testSession)

  private lazy val readSide = new TestEntityReadSide(system, testSession)

  override def getAppendCount(id: String): Future[Long] = readSide.getAppendCount(id)

  override def afterAll(): Unit = {
    persistentEntityRegistry.gracefulShutdown(5.seconds)
    super.afterAll()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // FIXME, use a global connection / get it from the journal
    CouchbaseCluster.create()
      .authenticate("admin", "admin1")
      .openBucket("akka")
      .query(N1qlQuery.simple("delete from akka"))

  }

}

