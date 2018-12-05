/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.query.{Offset, PersistenceQuery}
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.couchbase.{CouchbaseBucketSetup, SuppressedLogging, TestActor}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class EventsByPersistenceIdSpec
    extends TestKit(
      ActorSystem(
        "EventsByPersistenceIdSpec",
        ConfigFactory.parseString("""
            couchbase-journal.read.page-size = 10
          """).withFallback(ConfigFactory.load())
      )
    )
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with CouchbaseBucketSetup
    with SuppressedLogging {

  protected override def afterAll(): Unit = {
    super.afterAll()
    shutdown(system)
  }

  val noMsgTimeout = 100.millis
  private val readOurOwnWritesTimeout = 10.seconds
  override implicit val patienceConfig = PatienceConfig(readOurOwnWritesTimeout)

  implicit val materializer = ActorMaterializer()
  lazy val queries: CouchbaseReadJournal =
    PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)

  var idCounter = 0
  def nextPersistenceId(): String = {
    idCounter += 1
    val id = Integer.toString(idCounter, 24)
    id.toString
  }

  // provides a unique persistence-id per test case and some initial persisted events
  private trait Setup {
    lazy val probe = TestProbe()
    implicit def sender: ActorRef = probe.ref
    // note must be a def or lazy val or else it doesn't work (init order)
    def initialPersistedEvents: Int = 0
    def startPersistentActor(initialEvents: Int): (String, ActorRef) = {
      val pid = nextPersistenceId()
      system.log.debug("Starting actor with pid {}, and writing {} initial events", pid, initialPersistedEvents)
      val persistentActor = system.actorOf(TestActor.props(pid))
      if (initialEvents > 0) {
        for (i <- 1 to initialEvents) {
          persistentActor ! s"$pid-$i"
          probe.expectMsg(s"$pid-$i-done")
        }
      }
      (pid, persistentActor)
    }
    val (pid, persistentActor) = startPersistentActor(initialPersistedEvents)

    // no guarantee we can immediately read our own writes
    def readingOurOwnWrites[A](f: => A): A =
      awaitAssert(f, readOurOwnWritesTimeout)
  }

  "Couchbase query EventsByPersistenceId" must {

    "find existing events" in new Setup {
      override def initialPersistedEvents = 3

      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(2)
          .expectNext(s"$pid-1", s"$pid-2")
          .expectNoMessage(noMsgTimeout)
          .request(2)
          .expectNext(s"$pid-3")
          .expectComplete()
      }
    }

    "skip deleted events" in new Setup {
      override def initialPersistedEvents = 3
      persistentActor ! TestActor.DeleteTo(2)
      probe.expectMsg(DeleteMessagesSuccess(2))
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(2)
          .expectNext(s"$pid-3")
          .expectComplete()
      }
    }

    "find existing events from a sequence number" in new Setup {
      override def initialPersistedEvents = 10

      val src = queries.currentEventsByPersistenceId(pid, 5L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.sequenceNr)
          .runWith(TestSink.probe[Any])
          .request(7)
          .expectNext(5, 6, 7, 8, 9, 10)
          .expectComplete()
      }
    }

    "not see any events if the stream starts after current latest event" in new Setup {
      override def initialPersistedEvents = 3
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 5L, Long.MaxValue)
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(5)
          .expectComplete()
      }
    }

    "find existing events up to a sequence number" in new Setup {
      override def initialPersistedEvents = 3
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, 2L)
        src
          .map(_.sequenceNr)
          .runWith(TestSink.probe[Any])
          .request(5)
          .expectNext(1, 2)
          .expectComplete()
      }
    }

    "not see new events after demand request" in new Setup {
      override def initialPersistedEvents = 3

      val streamProbe =
        readingOurOwnWrites {
          val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
          src
            .map(_.event)
            .runWith(TestSink.probe[Any])
            .request(2)
            .expectNext(s"$pid-1", s"$pid-2")
            .expectNoMessage(noMsgTimeout)
        }

      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      // FIXME this sometimes sees completion instead of that third element
      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext(s"$pid-3")
        .expectComplete() // e-4 not seen

    }

    "only deliver what requested if there is more in the buffer" in new Setup {
      override def initialPersistedEvents = 1000
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
        val streamProbe = src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(2)
          .expectNext(s"$pid-1", s"$pid-2")
          .expectNoMessage(noMsgTimeout)

        streamProbe
          .expectNoMessage(noMsgTimeout)
          .request(5)
          .expectNext(s"$pid-3", s"$pid-4", s"$pid-5", s"$pid-6", s"$pid-7")
          .expectNoMessage(noMsgTimeout)

        streamProbe
          .request(5)
          .expectNext(s"$pid-8", s"$pid-9", s"$pid-10", s"$pid-11", s"$pid-12")
          .expectNoMessage(noMsgTimeout)
      }
    }

    "stop if there are no events" in new Setup {
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)

      src
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectComplete()
    }

    "produce correct sequence of sequence numbers and offsets" in new Setup {
      override def initialPersistedEvents = 3
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(x => (x.persistenceId, x.sequenceNr, x.offset))
          .runWith(TestSink.probe[Any])
          .request(4)
          .expectNext((pid, 1, Offset.sequence(1)), (pid, 2, Offset.sequence(2)), (pid, 3, Offset.sequence(3)))
          .expectComplete()
      }
    }

    // these talk about partitions, are they straight copy pasta from cassandra? We don't have partitions.
    "produce correct sequence of events across multiple partitions" in new Setup {
      override def initialPersistedEvents = 20
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(10)
          .expectNextN((1 to 10).map(i => s"$pid-$i"))
          .expectNoMessage(noMsgTimeout)
          .request(11)
          .expectNextN((11 to 20).map(i => s"$pid-$i"))
          .expectComplete()
      }
    }

    "stop at last event in partition" in new Setup {
      override def initialPersistedEvents = 15 // partition size is 15
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(100)
          .expectNextN((1 to 15).map(i => s"$pid-$i"))
          .expectComplete()
      }
    }

    "find all events when PersistAll spans partition boundaries" in new Setup {
      override def initialPersistedEvents = 10

      // partition 0
      persistentActor ! TestActor.PersistAll((11 to 15).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // partition 1
      persistentActor ! TestActor.PersistAll((16 to 17).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // all these go into partition 2
      persistentActor ! TestActor.PersistAll((18 to 31).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      persistentActor ! s"$pid-32"
      probe.expectMsg(s"$pid-32-done")

      awaitAssert(
        {
          val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
          src
            .map(_.event)
            .runWith(TestSink.probe[Any])
            .request(10)
            .expectNextN((1 to 10).map(i => s"$pid-$i"))
            .expectNoMessage(noMsgTimeout)
            .request(10)
            .expectNextN((11 to 20).map(i => s"$pid-$i"))
            .request(100)
            .expectNextN((21 to 32).map(i => s"$pid-$i"))
            .expectComplete()
        },
        readOurOwnWritesTimeout
      )
    }

    "complete when same number of events as page size" in new Setup {
      override def initialPersistedEvents = queries.settings.pageSize
      val pageSize = initialPersistedEvents
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
        val streamProbe = src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(pageSize + 10L)

        streamProbe.expectNextN(pageSize.toLong) should ===((1 to pageSize).map(n => s"$pid-$n").toList)
        streamProbe.expectComplete()
      }
    }

  }

  "Couchbase live query EventsByPersistenceId" must {

    "find new events" in new Setup {
      override def initialPersistedEvents = 3
      val streamProbe = readingOurOwnWrites {
        val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(5)
          .expectNext(s"$pid-1", s"$pid-2", s"$pid-3")
      }
      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      streamProbe.expectNext(s"$pid-4")
      streamProbe.cancel()
    }

    "skip deleted events" in new Setup {
      override def initialPersistedEvents = 3
      persistentActor ! TestActor.DeleteTo(2)
      probe.expectMsg(DeleteMessagesSuccess(2))
      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe =
        readingOurOwnWrites {
          src
            .map(_.event)
            .runWith(TestSink.probe[Any])
            .request(2)
            .expectNext(s"$pid-3")
            .expectNoMessage(noMsgTimeout)
        }

      streamProbe.cancel()
    }

    "find new events if the stream starts after current latest event" in new Setup {
      override def initialPersistedEvents = 4

      val src = queries.eventsByPersistenceId(pid, 5L, Long.MaxValue)
      val streamProbe = src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMessage(noMsgTimeout)

      persistentActor ! s"$pid-5"
      probe.expectMsg(s"$pid-5-done")
      persistentActor ! s"$pid-6"
      probe.expectMsg(s"$pid-6-done")

      streamProbe.expectNext(5, 6)

      persistentActor ! s"$pid-7"
      probe.expectMsg(s"$pid-7-done")

      streamProbe.expectNext(7)
      streamProbe.cancel()
    }

    "find new events up to a sequence number" in new Setup {
      override def initialPersistedEvents = 3

      val src = queries.eventsByPersistenceId(pid, 0L, 4L)
      val streamProbe = src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2, 3)

      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      streamProbe
        .expectNext(4)
        .expectComplete()
    }

    "find new events after demand request" in new Setup {
      override def initialPersistedEvents = 3

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext(s"$pid-1", s"$pid-2")
        .expectNoMessage(noMsgTimeout)

      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext(s"$pid-3")
        .expectNext(s"$pid-4")

      streamProbe.cancel()
    }

    "only deliver what requested if there is more in the buffer" in new Setup {
      override def initialPersistedEvents = 1000

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext(s"$pid-1", s"$pid-2")
        .expectNoMessage(noMsgTimeout)

      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext(s"$pid-3", s"$pid-4", s"$pid-5", s"$pid-6", s"$pid-7")
        .expectNoMessage(noMsgTimeout)

      streamProbe
        .request(5)
        .expectNext(s"$pid-8", s"$pid-9", s"$pid-10", s"$pid-11", s"$pid-12")
        .expectNoMessage(noMsgTimeout)

      streamProbe.cancel()
    }

    "not produce anything if there aren't any events" in new Setup {
      override def initialPersistedEvents = 1 // Database init.

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)

      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNoMessage(noMsgTimeout)

      streamProbe.cancel()
    }

    "not produce anything until there are existing events" in new Setup {
      override def initialPersistedEvents = 1 // one existing for actor 1

      // just making sure there are other actor events in the journal
      readingOurOwnWrites {
        val events = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue).runWith(Sink.seq).futureValue
        events should have size (1)
      }

      val (pid2, persistentActor2) = startPersistentActor(0)

      // start the live query before there are any events for actor 2
      val actor2Probe = queries
        .eventsByPersistenceId(pid2, 0L, Long.MaxValue)
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNoMessage(noMsgTimeout)

      // and then write some events (to both actors)
      persistentActor2 ! s"$pid2-1"
      probe.expectMsg(s"$pid2-1-done")
      persistentActor ! s"$pid-2"
      probe.expectMsg(s"$pid-2-done")
      persistentActor2 ! s"$pid2-2"
      probe.expectMsg(s"$pid2-2-done")
      persistentActor ! s"$pid-3"
      probe.expectMsg(s"$pid-3-done")

      // and we should see the events only for actor 2
      actor2Probe
        .expectNext(s"$pid2-1")
        .expectNext(s"$pid2-2")
        .expectNoMessage(noMsgTimeout)

      actor2Probe.cancel()
    }

    "produce correct sequence of events across multiple partitions" in new Setup {
      override def initialPersistedEvents = 15

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)

      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(16)
        .expectNextN((1 to 15).map(i => s"$pid-$i"))
        .expectNoMessage(noMsgTimeout)

      for (i <- 16 to 21) {
        persistentActor ! s"$pid-$i"
        probe.expectMsg(s"$pid-$i-done")
      }

      streamProbe
        .request(6)
        .expectNextN((16 to 21).map(i => s"$pid-$i"))

      for (i <- 22 to 35) {
        persistentActor ! s"$pid-$i"
        probe.expectMsg(s"$pid-$i-done")
      }

      streamProbe
        .request(10)
        .expectNextN((22 to 31).map(i => s"$pid-$i"))

      streamProbe.cancel()
    }

    "find all events when PersistAll spans partition boundaries" in new Setup {
      override def initialPersistedEvents = 10

      // partition 0
      persistentActor ! TestActor.PersistAll((11 to 15).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // partition 1
      persistentActor ! TestActor.PersistAll((16 to 17).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // all these go into partition 2
      persistentActor ! TestActor.PersistAll((18 to 31).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      persistentActor ! s"$pid-32"
      probe.expectMsg(s"$pid-32-done")

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNextN((1 to 10).map(i => s"$pid-$i"))
        .expectNoMessage(noMsgTimeout)
        .request(10)
        .expectNextN((11 to 20).map(i => s"$pid-$i"))
        .request(100)
        .expectNextN((21 to 32).map(i => s"$pid-$i"))

      // partition 2
      persistentActor ! TestActor.PersistAll((32 to 42).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      streamProbe.expectNextN((32 to 42).map(i => s"$pid-$i"))

      // still partition 2
      persistentActor ! s"$pid-43"
      probe.expectMsg(s"$pid-43-done")
      streamProbe.expectNext(s"$pid-43")

      // partition 3
      persistentActor ! TestActor.PersistAll((44 to 46).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      streamProbe.expectNextN((44 to 46).map(i => s"$pid-$i"))

      streamProbe.cancel()
    }
  }
}
