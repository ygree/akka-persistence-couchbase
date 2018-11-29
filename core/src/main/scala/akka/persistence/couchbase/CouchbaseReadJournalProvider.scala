/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.ExtendedActorSystem
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.{javadsl, ReadJournalProvider}
import com.typesafe.config.Config

final class CouchbaseReadJournalProvider(as: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {
  override def scaladslReadJournal(): ReadJournal = new CouchbaseReadJournal(as, config, configPath)
  // FIXME todo
  override def javadslReadJournal(): javadsl.ReadJournal = new javadsl.ReadJournal {}
}
