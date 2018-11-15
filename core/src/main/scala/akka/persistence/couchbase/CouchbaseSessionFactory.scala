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

  private var session: Future[CouchbaseSession] = _

  def apply(system: ActorSystem,
            sessionSettings: CouchbaseSessionSettings,
            bucket: String,
            indexAutoCreate: Boolean): Future[CouchbaseSession] = {

    import system.dispatcher

    val log = system.log

    synchronized {
      if (session == null) {
        session = CouchbaseSession.async(sessionSettings, bucket) //TODO use asyncCluster and connect asynchronously
      }
    }

//    if (!indexAutoCreate) {
//      session
//    } else {
    val future = session.flatMap(_.createIndex("pi2", true, "persistence_id", "sequence_from"))

    future.onComplete {
      case Success(true) =>
        log.info("Indexes have been created successfully.")
      case Success(false) =>
        log.info("Indexes already exist.")
      case Failure(t) =>
        log.error(t, "Couldn't create indexes")
    }

    future.flatMap(_ => session)
//    }
  }

}
