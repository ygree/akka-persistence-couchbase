package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import java.util.Optional

import javax.inject.{Inject, Singleton}
import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.couchbase.CouchbaseReadJournal
import com.lightbend.lagom.internal.javadsl.persistence.AbstractPersistentEntityRegistry
import play.api.inject.Injector

/**
  * Internal API
  */
@Singleton
private[lagom] final class CouchbasePersistentEntityRegistry @Inject()(system: ActorSystem, injector: Injector)
  extends AbstractPersistentEntityRegistry(system, injector) {

  private val log = Logging.getLogger(system, getClass)

  //TODO: validate Couchbase config

  override protected val queryPluginId = Optional.of(CouchbaseReadJournal.Identifier)

}

