/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

import scala.concurrent.Await
import scala.util.{Failure, Success}

object CouchbaseSessionFactory {

  def apply(system: ActorSystem,
            sessionSettings: CouchbaseSessionSettings,
            bucket: String,
            indexAutoCreate: Boolean = true): CouchbaseSession = {

    val log = system.log

//    implicit val ec = system.dispatcher //TODO use another execution context for blocking operations

    //TODO
    val session = CouchbaseSession(sessionSettings, bucket)

    import scala.concurrent.duration._

    if (indexAutoCreate) {
      val future = session.createIndex("pi2", true, "persistence_id", "sequence_from")

      Await
        .ready(future, 30.seconds)
//      future
        .onComplete {
          case Success(true) =>
            log.info("Indexes have been created successfully.")
          case Success(false) =>
            log.info("Indexes already exist.")
          case Failure(t) =>
            log.error(t, "Couldn't create indexes")
        }(ExecutionContexts.sameThreadExecutionContext)
    }

    session
  }

}
