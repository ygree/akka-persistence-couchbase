/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import java.lang
import java.util.concurrent.CompletionStage

import akka.persistence.couchbase.CouchbaseBucketSetup
import com.lightbend.lagom.internal.javadsl.persistence.couchbase.{
  CouchbasePersistentEntityRegistry,
  CouchbaseReadSideImpl,
  JavadslCouchbaseOffsetStore
}
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.javadsl.persistence.{AbstractReadSideSpec, ReadSideProcessor, TestEntity}
import com.typesafe.config.{Config, ConfigFactory}
import play.api.inject.guice.GuiceInjectorBuilder
import com.lightbend.lagom.javadsl.persistence.TestEntityReadSide

object CouchbaseReadSideSpec {

  val defaultConfig: Config = ConfigFactory.parseString("akka.loglevel = INFO")
}

class CouchbaseReadSideSpec
    extends CouchbasePersistenceSpec(CouchbaseReadSideSpec.defaultConfig)
    with AbstractReadSideSpec
    with CouchbaseBucketSetup {

  private lazy val injector = new GuiceInjectorBuilder().build()

  lazy val testSession = couchbaseSession.asJava

  override protected lazy val persistentEntityRegistry = new CouchbasePersistentEntityRegistry(system, injector)

  private lazy val offsetStore = new JavadslCouchbaseOffsetStore(system, testSession, ReadSideConfig())
  private lazy val couchbaseReadSide = new CouchbaseReadSideImpl(system, testSession, offsetStore, injector)

  override def processorFactory(): ReadSideProcessor[TestEntity.Evt] =
    new TestEntityReadSide.TestEntityReadSideProcessor(couchbaseReadSide)

  private lazy val readSide = new TestEntityReadSide(testSession)

  override def getAppendCount(id: String): CompletionStage[lang.Long] = readSide.getAppendCount(id)
}
