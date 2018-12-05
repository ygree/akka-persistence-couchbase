/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

final class CouchbaseReadJournalProvider(as: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {

  override val scaladslReadJournal: scaladsl.CouchbaseReadJournal =
    new scaladsl.CouchbaseReadJournal(as, config, configPath)

  override def javadslReadJournal(): javadsl.CouchbaseReadJournal =
    new javadsl.CouchbaseReadJournal(scaladslReadJournal)
}
