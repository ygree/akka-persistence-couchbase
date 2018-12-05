/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.couchbase.scaladsl.CouchbaseReadJournal
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseConfigValidator
import com.lightbend.lagom.internal.scaladsl.persistence.AbstractPersistentEntityRegistry

/**
 * Internal API
 */
private[lagom] final class CouchbasePersistentEntityRegistry(system: ActorSystem)
    extends AbstractPersistentEntityRegistry(system) {

  private val log = Logging.getLogger(system, getClass)

  CouchbaseConfigValidator.validateBucket("couchbase-journal.write", system.settings.config, log)
  CouchbaseConfigValidator.validateBucket("couchbase-journal.snapshot", system.settings.config, log)

  override protected val queryPluginId = Some(CouchbaseReadJournal.Identifier)
}
