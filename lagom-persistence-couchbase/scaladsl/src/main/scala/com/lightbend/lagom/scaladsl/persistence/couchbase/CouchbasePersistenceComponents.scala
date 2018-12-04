/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import akka.event.Logging
import akka.stream.alpakka.couchbase.{scaladsl, CouchbaseSessionRegistry, CouchbaseSessionSettings}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.lightbend.lagom.internal.persistence.couchbase.{CouchbaseConfigValidator, CouchbaseOffsetStore}
import com.lightbend.lagom.internal.scaladsl.persistence.couchbase.{
  CouchbasePersistentEntityRegistry,
  CouchbaseReadSideImpl,
  ScaladslCouchbaseOffsetStore
}
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.persistence.{
  PersistenceComponents,
  PersistentEntityRegistry,
  ReadSidePersistenceComponents,
  WriteSidePersistenceComponents
}
import com.lightbend.lagom.spi.persistence.OffsetStore
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.Await

/**
 * Persistence Couchbase components (for compile-time injection).
 */
trait CouchbasePersistenceComponents
    extends PersistenceComponents
    with ReadSideCouchbasePersistenceComponents
    with WriteSideCouchbasePersistenceComponents

/**
 * Write-side persistence Couchbase components (for compile-time injection).
 */
trait WriteSideCouchbasePersistenceComponents extends WriteSidePersistenceComponents {

  override lazy val persistentEntityRegistry: PersistentEntityRegistry =
    new CouchbasePersistentEntityRegistry(actorSystem)

  def serviceLocator: ServiceLocator

}

/**
 * Read-side persistence Couchbase components (for compile-time injection).
 */
trait ReadSideCouchbasePersistenceComponents extends ReadSidePersistenceComponents {

  private val log = Logging(actorSystem, classOf[ReadSideCouchbasePersistenceComponents])

  CouchbaseConfigValidator.validateBucket("lagom.persistence.read-side.couchbase", configuration.underlying, log)

  private val readSideCouchbaseConfig: Config =
    configuration.underlying.getConfig("lagom.persistence.read-side.couchbase")

  private val sessionSettings = CouchbaseSessionSettings(
    readSideCouchbaseConfig.getConfig("connection")
  )

  private val bucket = readSideCouchbaseConfig.getString("bucket")

  // FIXME is there a way to have async component creation in lagom instead of letting every component know that the thing is async?
  // if not we should pass Future[CouchbaseSession] around and let the use sites mix in AsyncCouchbaseSession - but if we use
  // that from Lagom it needs to be made public API
  lazy val couchbase: CouchbaseSession =
    Await.result(CouchbaseSessionRegistry(actorSystem).sessionFor(sessionSettings, bucket), 30.seconds)

  private[lagom] lazy val couchbaseOffsetStore: CouchbaseOffsetStore =
    new ScaladslCouchbaseOffsetStore(actorSystem, couchbase, readSideConfig)

  lazy val offsetStore: OffsetStore = couchbaseOffsetStore

  lazy val couchbaseReadSide: CouchbaseReadSide =
    new CouchbaseReadSideImpl(actorSystem, couchbase, couchbaseOffsetStore)
}
