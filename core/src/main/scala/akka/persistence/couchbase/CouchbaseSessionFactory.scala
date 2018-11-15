/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * Internal API
 */
@InternalApi
object CouchbaseSessionFactory {

  private var session: CouchbaseSession = _

  def apply(system: ActorSystem,
            sessionSettings: CouchbaseSessionSettings,
            bucket: String,
            indexAutoCreate: Boolean): Future[CouchbaseSession] = {

    val log = system.log

    synchronized {
      if (session == null) {
        session = CouchbaseSession(sessionSettings, bucket) //TODO use asyncCluster and connect asynchronously
      }
    }

    if (indexAutoCreate) {
      val future = session.createIndex("pi2", true, "persistence_id", "sequence_from")

      future.onComplete {
        case Success(true) =>
          log.info("Indexes have been created successfully.")
        case Success(false) =>
          log.info("Indexes already exist.")
        case Failure(t) =>
          log.error(t, "Couldn't create indexes")
      }(ExecutionContexts.sameThreadExecutionContext)

      return future.map(_ => session)(system.dispatcher)
    }

    Future.successful(session)
  }

}
