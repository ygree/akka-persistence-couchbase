/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase

import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._

class CouchbaseSettingsSpec extends WordSpec with Matchers {

  "The couchbase session settings" must {

    "parse config into settings" in {
      val config = ConfigFactory.parseString("""
          username=scott
          password=tiger
          nodes=["example.com:2323"]
        """)

      val settingss = Seq(CouchbaseSessionSettings(config), CouchbaseSessionSettings.create(config))
      settingss.foreach { settings =>
        settings.username should ===("scott")
        settings.password should ===("tiger")
        settings.nodes should ===(List("example.com:2323"))
      }
    }

    "be changeable using 'with' methods" in {
      val modified = CouchbaseSessionSettings("scott", "tiger")
        .withNodes("example.com:123")
        .withUsername("bob")
        .withPassword("lynx")

      modified.username should ===("bob")
      modified.password should ===("lynx")
      modified.nodes should ===("example.com:123" :: Nil)
    }

  }

  "The couchbase write settings" must {

    "be created from both java and scala APIs" in {
      val scala = CouchbaseWriteSettings(1, ReplicateTo.NONE, PersistTo.THREE, 10.seconds)
      val java =
        CouchbaseWriteSettings.create(1, ReplicateTo.NONE, PersistTo.THREE, _root_.java.time.Duration.ofSeconds(10))

      scala should ===(java)
    }

  }

}
