/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.couchbase.CouchbaseReadJournal
import com.lightbend.lagom.internal.scaladsl.persistence.AbstractPersistentEntityRegistry

/**
 * Internal API
 */
private[lagom] final class CouchbasePersistentEntityRegistry(system: ActorSystem)
  extends AbstractPersistentEntityRegistry(system) {

  private val log = Logging.getLogger(system, getClass)

  override protected val queryPluginId = Some(CouchbaseReadJournal.Identifier)
}
