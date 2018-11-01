package com.lightbend.lagom.javadsl.persistence.couchbase

import java.net.URI
import java.{lang, util}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.persistence.couchbase.CouchbaseSettings
import com.couchbase.client.core.ClusterFacade
import com.couchbase.client.core.message.internal.PingReport
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.java.analytics.{AnalyticsQuery, AsyncAnalyticsQueryResult}
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.datastructures.MutationOptionBuilder
import com.couchbase.client.java._
import com.couchbase.client.java.document.{Document, JsonDocument, JsonLongDocument}
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, N1qlQuery, Statement}
import com.couchbase.client.java.repository.AsyncRepository
import com.couchbase.client.java.search.SearchQuery
import com.couchbase.client.java.search.result.AsyncSearchQueryResult
import com.couchbase.client.java.subdoc.{AsyncLookupInBuilder, AsyncMutateInBuilder}
import com.couchbase.client.java.transcoder.subdoc.FragmentTranscoder
import com.couchbase.client.java.view.{AsyncSpatialViewResult, AsyncViewResult, SpatialViewQuery, ViewQuery}
import com.google.inject.Provider
import com.lightbend.lagom.internal.javadsl.persistence.couchbase.{CouchbasePersistentEntityRegistry, JavadslCouchbaseOffsetStore}
import com.lightbend.lagom.internal.persistence.couchbase.{ServiceLocatorAdapter, ServiceLocatorHolder}
import com.lightbend.lagom.javadsl.api.ServiceLocator
import com.lightbend.lagom.javadsl.persistence.PersistentEntityRegistry
import com.lightbend.lagom.spi.persistence.OffsetStore
import com.typesafe.config.Config
import javax.annotation.PostConstruct
import javax.inject.Inject
import play.api.inject.{Binding, Injector, Module}
import play.api.{Configuration, Environment}
import rx.{Observable, Single}

import scala.concurrent.{ExecutionContext, Future}
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
    bind[AsyncBucket].toProvider[AsyncBucketProvider] //TODO:
  )

}

private[lagom] class AsyncBucketProvider @Inject()(cfg: Config) extends Provider[AsyncBucket] {
  private val config: CouchbaseSettings = CouchbaseSettings(cfg)
//  private implicit val ec: ExecutionContext = context.dispatcher

  private val cluster: Cluster = {
    val c = CouchbaseCluster.create()
//    log.info("Auth {} {}", config.username, config.password)
    c.authenticate(config.username, config.password)
    c
  }

  val bucket: Bucket = cluster.openBucket(config.bucket)
  val asyncBucket: AsyncBucket = cluster.openBucket(config.bucket).async()

  override def get(): AsyncBucket = {
    //TODO:
    asyncBucket
  }
}

private[lagom] object CouchbasePersistenceModule {

  class InitServiceLocatorHolder @Inject() (system: ActorSystem, injector: Injector) {

    // Guice doesn't support this, but other DI frameworks do.
    @PostConstruct
    def init(): Unit = {
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

}
