/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase


import com.couchbase.client.java.AsyncBucket
import com.lightbend.lagom.internal.scaladsl.persistence.couchbase.{ CouchbasePersistentEntityRegistry, CouchbaseReadSideImpl, ScaladslCouchbaseOffsetStore }
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.persistence.{ PersistenceComponents, PersistentEntityRegistry, ReadSidePersistenceComponents, WriteSidePersistenceComponents }
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore
import com.lightbend.lagom.spi.persistence.OffsetStore
/**
 * Persistence Couchbase components (for compile-time injection).
 */
trait CouchbasePersistenceComponents extends PersistenceComponents
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
  lazy val asyncBucket: AsyncBucket = ???

  private[lagom] lazy val couchbaseOffsetStore: CouchbaseOffsetStore =
    new ScaladslCouchbaseOffsetStore(actorSystem, asyncBucket, readSideConfig)(executionContext)

  lazy val offsetStore: OffsetStore = couchbaseOffsetStore

  lazy val couchbaseReadSide: CouchbaseReadSide = new CouchbaseReadSideImpl(actorSystem, asyncBucket, couchbaseOffsetStore)
}
