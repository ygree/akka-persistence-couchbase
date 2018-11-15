/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import akka.persistence.couchbase.CouchbaseJournalSettings
import akka.stream.alpakka.couchbase.scaladsl.Couchbase
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore
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

  private val settings: CouchbaseJournalSettings = CouchbaseJournalSettings(
    configuration.underlying.getConfig("couchbase-journal")
  )

  lazy val couchbase: Couchbase = Couchbase(settings.sessionSettings, settings.bucket)

  private[lagom] lazy val couchbaseOffsetStore: CouchbaseOffsetStore =
    new ScaladslCouchbaseOffsetStore(actorSystem, couchbase, readSideConfig)

  lazy val offsetStore: OffsetStore = couchbaseOffsetStore

  lazy val couchbaseReadSide: CouchbaseReadSide =
    new CouchbaseReadSideImpl(actorSystem, couchbase, couchbaseOffsetStore)
}
