/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import java.util.Optional

import javax.inject.{Inject, Singleton}
import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.couchbase.scaladsl.CouchbaseReadJournal
import com.lightbend.lagom.internal.javadsl.persistence.AbstractPersistentEntityRegistry
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseConfigValidator
import play.api.inject.Injector

/**
 * Internal API
 */
@Singleton
private[lagom] final class CouchbasePersistentEntityRegistry @Inject()(system: ActorSystem, injector: Injector)
    extends AbstractPersistentEntityRegistry(system, injector) {

  private val log = Logging.getLogger(system, getClass)

  CouchbaseConfigValidator.validateBucket("couchbase-journal.write", system.settings.config, log)
  CouchbaseConfigValidator.validateBucket("couchbase-journal.snapshot", system.settings.config, log)

  override protected val queryPluginId = Optional.of(CouchbaseReadJournal.Identifier)

}
