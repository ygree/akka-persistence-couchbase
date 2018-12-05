/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.couchbase.internal.UUIDTimestamp
import akka.persistence.query.{NoOffset, TimeBasedUUID}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.Checkers

class UUIDsSpec extends WordSpec with Matchers with ScalaFutures with Checkers {

  val timestamp = Gen.choose(
    UUIDTimestamp.MinVal.toUnixTimestamp,
    UUIDTimestamp.MaxVal.toUnixTimestamp / 10 // TODO: there is some overflow happening for bigger values
  ).filter(_ != 0)

  "UUIDs factory" should {
    "return NoOffset for zero timestamp" in {
      assert(UUIDs.timeBasedUUIDFrom(0) === NoOffset)
    }
    "return TimeBasedUUID for non-zero timestamp" in check(
      forAll(timestamp) { ts =>
        UUIDs.timeBasedUUIDFrom(ts).isInstanceOf[TimeBasedUUID]
      }
    )
    "preserve timestamp" in check(
      forAll(timestamp) { ts =>
        val uuid = UUIDs.timeBasedUUIDFrom(ts).asInstanceOf[TimeBasedUUID]
        UUIDs.timestampFrom(uuid) === ts
      }
    )
  }
}
