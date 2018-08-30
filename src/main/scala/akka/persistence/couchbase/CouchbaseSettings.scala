package akka.persistence.couchbase

import com.typesafe.config.Config

class CouchbaseSettings private(config: Config) {
  val bucket = "akka"
  val username = "admin"
  val password = "admin1"

}

object CouchbaseSettings {
  def apply(config: Config): CouchbaseSettings = new CouchbaseSettings(config)
}
