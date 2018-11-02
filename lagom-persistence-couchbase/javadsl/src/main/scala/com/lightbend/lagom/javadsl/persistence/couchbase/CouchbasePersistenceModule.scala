/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import java.net.URI

import akka.actor.ActorSystem
import akka.persistence.couchbase.CouchbaseJournalSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.google.inject.Provider
import com.lightbend.lagom.internal.javadsl.persistence.couchbase.{
  CouchbasePersistentEntityRegistry,
  JavadslCouchbaseOffsetStore
}
import com.lightbend.lagom.internal.persistence.couchbase.{ServiceLocatorAdapter, ServiceLocatorHolder}
import com.lightbend.lagom.javadsl.api.ServiceLocator
import com.lightbend.lagom.javadsl.persistence.PersistentEntityRegistry
import com.lightbend.lagom.spi.persistence.OffsetStore
import com.typesafe.config.Config
import javax.annotation.PostConstruct
import javax.inject.Inject
import play.api.inject.{Binding, Injector, Module}
import play.api.{Configuration, Environment}

import scala.concurrent.Future
import scala.util.Try

/**
 * Guice module for the Persistence API.
 */
class CouchbasePersistenceModule extends Module {

  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] = Seq(
    bind[CouchbasePersistenceModule.InitServiceLocatorHolder].toSelf.eagerly(),
    bind[PersistentEntityRegistry].to[CouchbasePersistentEntityRegistry],
    //TODO: add other modules similar to Cassandra
    //    bind[CassandraSession].toSelf,
    //    bind[CassandraReadSide].to[CassandraReadSideImpl],
    //    bind[CassandraReadSideSettings].toSelf,
    //    bind[CassandraOffsetStore].to[JavadslCassandraOffsetStore],
    bind[OffsetStore].to(bind[JavadslCouchbaseOffsetStore]),
    bind[CouchbaseSession].toProvider[CouchbaseSessionProvider] //TODO:
  )

}

private[lagom] class CouchbaseSessionProvider @Inject()(cfg: Config) extends Provider[CouchbaseSession] {

  private val settings: CouchbaseJournalSettings = CouchbaseJournalSettings(cfg)
  //  private implicit val ec: ExecutionContext = context.dispatcher

  val session = CouchbaseSession(settings.sessionSettings, settings.bucket)

  override def get(): CouchbaseSession =
    //TODO:
    session
}

private[lagom] object CouchbasePersistenceModule {

  class InitServiceLocatorHolder @Inject()(system: ActorSystem, injector: Injector) {

    // Guice doesn't support this, but other DI frameworks do.
    @PostConstruct
    def init(): Unit =
      Try(injector.instanceOf[ServiceLocator]).foreach { locator =>
        ServiceLocatorHolder(system).setServiceLocator(new ServiceLocatorAdapter {
          override def locateAll(name: String): Future[List[URI]] = {
            import system.dispatcher

            import scala.collection.JavaConverters._
            import scala.compat.java8.FutureConverters._
            locator.locateAll(name).toScala.map(_.asScala.toList)
          }
        })
      }
  }

}
