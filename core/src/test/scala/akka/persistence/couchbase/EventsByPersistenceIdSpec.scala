/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.query.{Offset, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class EventsByPersistenceIdSpec
    extends TestKit(ActorSystem("EventsByPersistenceIdSpec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ImplicitSender
    with CouchbaseBucketSetup
    with SuppressedLogging {

  protected override def afterAll(): Unit = {
    super.afterAll()
    shutdown(system)
  }

  // TODO run this test with smaller pageSize, when that is configurable

  val noMsgTimeout = 100.millis

  implicit val materializer = ActorMaterializer()
  lazy val queries: CouchbaseReadJournal =
    PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)

  def setup(persistenceId: String, n: Int): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for (i <- 1 to n) {
      ref ! s"$persistenceId-$i"
      expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  "Couchbase query EventsByPersistenceId" must {

    "find existing events" in {
      setup("a", 3)

      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("a-1", "a-2")
        .expectNoMessage(noMsgTimeout)
        .request(2)
        .expectNext("a-3")
        .expectComplete()
    }

    "find existing events from a sequence number" in {
      setup("b", 10)
      val src = queries.currentEventsByPersistenceId("b", 5L, Long.MaxValue)

      src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(7)
        .expectNext(5, 6, 7, 8, 9, 10)
        .expectComplete()
    }

    "not see any events if the stream starts after current latest event" in {
      setup("c", 3)
      val src = queries.currentEventsByPersistenceId("c", 5L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectComplete()
    }

    "find existing events up to a sequence number" in {
      setup("d", 3)
      val src = queries.currentEventsByPersistenceId("d", 0L, 2L)
      src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2)
        .expectComplete()
    }

    "not see new events after demand request" in {
      val ref = setup("e", 3)

      val src = queries.currentEventsByPersistenceId("e", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("e-1", "e-2")
        .expectNoMessage(noMsgTimeout)

      ref ! "e-4"
      expectMsg("e-4-done")

      probe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext("e-3")
        .expectComplete() // e-4 not seen
    }

    "only deliver what requested if there is more in the buffer" in {
      setup("f", 1000)

      val src = queries.currentEventsByPersistenceId("f", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("f-1", "f-2")
        .expectNoMessage(noMsgTimeout)

      probe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext("f-3", "f-4", "f-5", "f-6", "f-7")
        .expectNoMessage(noMsgTimeout)

      probe
        .request(5)
        .expectNext("f-8", "f-9", "f-10", "f-11", "f-12")
        .expectNoMessage(noMsgTimeout)
    }

    "stop if there are no events" in {
      val src = queries.currentEventsByPersistenceId("g", 0L, Long.MaxValue)

      src
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectComplete()
    }

    "produce correct sequence of sequence numbers and offsets" in {
      setup("h", 3)

      val src = queries.currentEventsByPersistenceId("h", 0L, Long.MaxValue)
      src
        .map(x => (x.persistenceId, x.sequenceNr, x.offset))
        .runWith(TestSink.probe[Any])
        .request(4)
        .expectNext(("h", 1, Offset.sequence(1)), ("h", 2, Offset.sequence(2)), ("h", 3, Offset.sequence(3)))
        .expectComplete()
    }

    "produce correct sequence of events across multiple partitions" in {
      setup("i", 20)

      val src = queries.currentEventsByPersistenceId("i", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNextN((1 to 10).map(i => s"i-$i"))
        .expectNoMessage(noMsgTimeout)
        .request(11)
        .expectNextN((11 to 20).map(i => s"i-$i"))
        .expectComplete()
    }

    "stop at last event in partition" in {
      setup("i2", 15) // partition size is 15

      val src = queries.currentEventsByPersistenceId("i2", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(100)
        .expectNextN((1 to 15).map(i => s"i2-$i"))
        .expectComplete()
    }

    "find all events when PersistAll spans partition boundaries" in {
      val ref = setup("i3", 10)
      // partition 0
      ref ! TestActor.PersistAll((11 to 15).map(i => s"i3-$i"))
      expectMsg("PersistAll-done")
      // partition 1
      ref ! TestActor.PersistAll((16 to 17).map(i => s"i3-$i"))
      expectMsg("PersistAll-done")
      // all these go into partition 2
      ref ! TestActor.PersistAll((18 to 31).map(i => s"i3-$i"))
      expectMsg("PersistAll-done")
      ref ! "i3-32"
      expectMsg("i3-32-done")

      val src = queries.currentEventsByPersistenceId("i3", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNextN((1 to 10).map(i => s"i3-$i"))
        .expectNoMessage(noMsgTimeout)
        .request(10)
        .expectNextN((11 to 20).map(i => s"i3-$i"))
        .request(100)
        .expectNextN((21 to 32).map(i => s"i3-$i"))
        .expectComplete()
    }

    "complete when same number of events as page size" in {
      // TODO these hardcoded assignments of persistenceId is a mess, must be unique per test
      setup("a2", queries.pageSize)

      val src = queries.currentEventsByPersistenceId("a2", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(queries.pageSize + 10)

      probe.expectNextN(queries.pageSize) should ===((1 to queries.pageSize).map(n => s"a2-$n").toList)
      probe.expectComplete()
    }

  }

  "Couchbase live query EventsByPersistenceId" must {

    "find new events" in {
      val ref = setup("j", 3)
      val src = queries.eventsByPersistenceId("j", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("j-1", "j-2", "j-3")

      ref ! "j-4"
      expectMsg("j-4-done")

      probe.expectNext("j-4")
      probe.cancel()
    }

    "find new events if the stream starts after current latest event" in {
      val ref = setup("k", 4)
      val src = queries.eventsByPersistenceId("k", 5L, Long.MaxValue)
      val probe = src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMessage(noMsgTimeout)

      ref ! "k-5"
      expectMsg("k-5-done")
      ref ! "k-6"
      expectMsg("k-6-done")

      probe.expectNext(5, 6)

      ref ! "k-7"
      expectMsg("k-7-done")

      probe.expectNext(7)
      probe.cancel()
    }

    "find new events up to a sequence number" in {
      val ref = setup("l", 3)
      val src = queries.eventsByPersistenceId("l", 0L, 4L)
      val probe = src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2, 3)

      ref ! "l-4"
      expectMsg("l-4-done")

      probe
        .expectNext(4)
        .expectComplete()
    }

    "find new events after demand request" in {
      val ref = setup("m", 3)
      val src = queries.eventsByPersistenceId("m", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("m-1", "m-2")
        .expectNoMessage(noMsgTimeout)

      ref ! "m-4"
      expectMsg("m-4-done")

      probe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext("m-3")
        .expectNext("m-4")

      probe.cancel()
    }

    "only deliver what requested if there is more in the buffer" in {
      setup("n", 1000)

      val src = queries.eventsByPersistenceId("n", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("n-1", "n-2")
        .expectNoMessage(noMsgTimeout)

      probe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext("n-3", "n-4", "n-5", "n-6", "n-7")
        .expectNoMessage(noMsgTimeout)

      probe
        .request(5)
        .expectNext("n-8", "n-9", "n-10", "n-11", "n-12")
        .expectNoMessage(noMsgTimeout)

      probe.cancel()
    }

    "not produce anything if there aren't any events" in {
      setup("o2", 1) // Database init.
      val src = queries.eventsByPersistenceId("o", 0L, Long.MaxValue)

      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNoMessage(noMsgTimeout)

      probe.cancel()
    }

    "not produce anything until there are existing events" in {
      setup("p2", 1) // Database init.
      val src = queries.eventsByPersistenceId("p", 0L, Long.MaxValue)

      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNoMessage(noMsgTimeout)

      setup("p", 2)

      probe
        .expectNext("p-1", "p-2")
        .expectNoMessage(noMsgTimeout)

      probe.cancel()
    }

    "produce correct sequence of events across multiple partitions" in {
      val ref = setup("q", 15)

      val src = queries.eventsByPersistenceId("q", 0L, Long.MaxValue)

      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(16)
        .expectNextN((1 to 15).map(i => s"q-$i"))
        .expectNoMessage(noMsgTimeout)

      for (i <- 16 to 21) {
        ref ! s"q-$i"
        expectMsg(s"q-$i-done")
      }

      probe
        .request(6)
        .expectNextN((16 to 21).map(i => s"q-$i"))

      for (i <- 22 to 35) {
        ref ! s"q-$i"
        expectMsg(s"q-$i-done")
      }

      probe
        .request(10)
        .expectNextN((22 to 31).map(i => s"q-$i"))

      probe.cancel()
    }

    "find all events when PersistAll spans partition boundaries" in {
      val ref = setup("q2", 10)
      // partition 0
      ref ! TestActor.PersistAll((11 to 15).map(i => s"q2-$i"))
      expectMsg("PersistAll-done")
      // partition 1
      ref ! TestActor.PersistAll((16 to 17).map(i => s"q2-$i"))
      expectMsg("PersistAll-done")
      // all these go into partition 2
      ref ! TestActor.PersistAll((18 to 31).map(i => s"q2-$i"))
      expectMsg("PersistAll-done")
      ref ! "q2-32"
      expectMsg("q2-32-done")

      val src = queries.eventsByPersistenceId("q2", 0L, Long.MaxValue)
      val probe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNextN((1 to 10).map(i => s"q2-$i"))
        .expectNoMessage(noMsgTimeout)
        .request(10)
        .expectNextN((11 to 20).map(i => s"q2-$i"))
        .request(100)
        .expectNextN((21 to 32).map(i => s"q2-$i"))

      // partition 2
      ref ! TestActor.PersistAll((32 to 42).map(i => s"q2-$i"))
      expectMsg("PersistAll-done")
      probe.expectNextN((32 to 42).map(i => s"q2-$i"))

      // still partition 2
      ref ! "q2-43"
      expectMsg("q2-43-done")
      probe.expectNext("q2-43")

      // partition 3
      ref ! TestActor.PersistAll((44 to 46).map(i => s"q2-$i"))
      expectMsg("PersistAll-done")
      probe.expectNextN((44 to 46).map(i => s"q2-$i"))

      probe.cancel()
    }
  }
}
