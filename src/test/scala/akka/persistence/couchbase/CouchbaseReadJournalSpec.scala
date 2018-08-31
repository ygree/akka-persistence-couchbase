package akka.persistence.couchbase

import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, NoOffset, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.query.N1qlQuery
import org.scalatest.{BeforeAndAfterAll, WordSpec, WordSpecLike}

import scala.concurrent.duration._

class CouchbaseReadJournalSpec extends TestKit(ActorSystem("CouchbaseReadJournalSpec"))
  with WordSpecLike with BeforeAndAfterAll with ImplicitSender {


  protected override def beforeAll(): Unit = {
    super.beforeAll()
    CouchbaseCluster.create()
      .authenticate("admin", "admin1")
      .openBucket("akka")
      .query(N1qlQuery.simple("delete from akka"))
  }


  lazy val queries: CouchbaseReadJournal =
    PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)

  implicit val mat = ActorMaterializer()


  "currentPersistenceIds" must {
    "work" in {

      val pa1 = system.actorOf(TestActor.props("p1"))
      pa1 ! "p1-evt-1"
      expectMsg("p1-evt-1-done")
      val pa2 = system.actorOf(TestActor.props("p2"))
      pa2 ! "p2-evt-1"
      expectMsg("p2-evt-1-done")

      val probe: TestSubscriber.Probe[String] = queries.currentPersistenceIds().runWith(TestSink.probe)

      probe.requestNext("p1")
      probe.requestNext("p2")
      probe.expectComplete()

    }
  }

  "currentEventsByTag" must {
    "find existing events" in {
      val a = system.actorOf(TestActor.props("a"))
      val b = system.actorOf(TestActor.props("b"))
      a ! "hello"
      expectMsg(20.seconds, s"hello-done")
      a ! TestActor.PersistAll(List("a green apple", "a black car"))
      expectMsg(s"PersistAll-done")
      a ! "a green banana"
      expectMsg(s"a green banana-done")
      b ! "a green leaf"
      expectMsg(s"a green leaf-done")

      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e@EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe.expectNextPF { case e@EventEnvelope(_, "a", 4L, "a green banana") => e }

      probe.expectNoMessage(500.millis)
      probe.request(2)
      probe.expectNextPF { case e@EventEnvelope(_, "b", 1L, "a green leaf") => e }
      probe.expectComplete()

      val blackSrc = queries.currentEventsByTag(tag = "black", offset = NoOffset)
      val probe2 = blackSrc.runWith(TestSink.probe[Any])
      probe2.request(5)
      probe2.expectNextPF { case e@EventEnvelope(_, "a", 3L, "a black car") => e }
      probe2.expectComplete()

      val appleSrc = queries.currentEventsByTag(tag = "apple", offset = NoOffset)
      val probe3 = appleSrc.runWith(TestSink.probe[Any])
      probe3.request(5)
      probe3.expectNextPF { case e@EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe3.expectComplete()
    }

  }

}
