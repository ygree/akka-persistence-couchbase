/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import akka.persistence.couchbase.TestActor
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe

import scala.concurrent.duration._

class PersistenceIdsQuerySpec extends AbstractQuerySpec("PersistenceIdsQuerySpec") {

  "currentPersistenceIds" must {
    "work" in {

      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref
      val pa1 = system.actorOf(TestActor.props("p1"))
      pa1 ! "p1-evt-1"
      senderProbe.expectMsg("p1-evt-1-done")
      val pa2 = system.actorOf(TestActor.props("p2"))
      pa2 ! "p2-evt-1"
      senderProbe.expectMsg("p2-evt-1-done")

      awaitAssert(
        {
          val probe: TestSubscriber.Probe[String] = queries.currentPersistenceIds().runWith(TestSink.probe)

          probe.requestNext("p1")
          probe.requestNext("p2")
          probe.expectComplete()
        },
        readOurOwnWritesTimeout
      )

    }
  }

  "live persistenceIds" must {
    "show new persistence ids" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref

      val queryProbe: TestSubscriber.Probe[String] =
        queries.persistenceIds().runWith(TestSink.probe)

      queryProbe.request(10)

      val pa3 = system.actorOf(TestActor.props("p3"))
      pa3 ! "p3-evt-1"
      senderProbe.expectMsg("p3-evt-1-done")

      awaitAssert({
        queryProbe.expectNext("p3")
      }, 5.seconds)

      val pa4 = system.actorOf(TestActor.props("p4"))
      pa4 ! "p4-evt-1"
      senderProbe.expectMsg("p4-evt-1-done")

      // we shouldn't see p3 again
      queryProbe.expectNext("p4")
      // also not after p4 (it could come out of order)
      queryProbe.expectNoMessage(noMsgTimeout)

    }
  }

}
