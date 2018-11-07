/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpec}

class CouchbaseSessionSettingsSpec extends WordSpec with Matchers {

  "The couchbase session settings" must {

    "parse config into settings" in {
      val config = ConfigFactory.parseString("""
          username=scott
          password=tiger
          nodes=["example.com:2323"]
        """)

      val settings = CouchbaseSessionSettings(config)
      settings.username should ===("scott")
      settings.password should ===("tiger")
      settings.nodes should ===(List("example.com:2323"))
    }

  }

}
