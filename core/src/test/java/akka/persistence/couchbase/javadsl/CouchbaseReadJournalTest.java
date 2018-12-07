/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.javadsl;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.persistence.couchbase.CouchbaseClusterConnection;
import akka.persistence.couchbase.TestActor;
import akka.persistence.journal.Tagged;
import akka.persistence.journal.WriteEventAdapter;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.Offset;
import akka.persistence.query.PersistenceQuery;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class CouchbaseReadJournalTest {

  static class TestTagger implements WriteEventAdapter {
    @Override
    public String manifest(Object event) {
      return "";
    }

    @Override
    public Object toJournal(Object event) {
      if (event instanceof String && ((String) event).startsWith("a")) {
        return new Tagged(event, Collections.singleton("a"));
      }
      return event;
    }
  }

  static Config config = ConfigFactory.parseString(
      "couchbase-journal.write.event-adapters { \n" +
          "  test-tagger = \"akka.persistence.couchbase.javadsl.CouchbaseReadJournalTest$TestTagger\" \n" +
          "} \n " +

          "couchbase-journal.write.event-adapter-bindings { \n" +
          "  \"java.lang.String\" = test-tagger \n" +
          "} \n"
  ).withFallback(ConfigFactory.load());

  static ActorSystem system;
  static ActorMaterializer mat;
  static CouchbaseReadJournal queries;
  static CouchbaseClusterConnection clusterConnection;
  static CouchbaseSession couchbaseSession;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("CouchbaseReadJournalTest", config);
    mat = ActorMaterializer.create(system);

    // #read-journal-access
    queries = PersistenceQuery.get(system)
        .getReadJournalFor(CouchbaseReadJournal.class, CouchbaseReadJournal.Identifier());
    // #read-journal-access

    clusterConnection = CouchbaseClusterConnection.connect()
        .cleanUp();

    couchbaseSession = clusterConnection.couchbaseSession().asJava();
  }

  @AfterClass
  public static void teardown() {
    queries = null;

    mat.shutdown();
    mat = null;

    TestKit.shutdownActorSystem(system);
    system = null;

    couchbaseSession.close();
    couchbaseSession = null;

    clusterConnection.close();
    clusterConnection = null;
  }

  @Test
  public void test01_startEventsByPersistenceIdQuery() {
    new TestKit(system) {{
      ActorRef a = system.actorOf(TestActor.props("a"));
      a.tell("a-1", getRef());
      expectMsg("a-1-done");

      awaitAssert(() -> {
        Source<String, NotUsed> src = queries
            .eventsByPersistenceId("a", 0L, Long.MAX_VALUE)
            .map(EventEnvelope::persistenceId);

        TestSubscriber.Probe<String> probe = src.runWith(TestSink.<String>probe(system), mat);
        probe.request(10);
        probe.expectNext("a");
        return probe.cancel();
      });
    }};
  }

  @Test
  public void test02_startCurrentEventsByPersistenceIdQuery() {
    new TestKit(system) {{
      ActorRef a = system.actorOf(TestActor.props("b"));
      a.tell("b-1", getRef());
      expectMsg("b-1-done");

      awaitAssert(() -> {
        Source<String, NotUsed> src = queries
            .currentEventsByPersistenceId("b", 0L, Long.MAX_VALUE)
            .map(EventEnvelope::persistenceId);

        TestSubscriber.Probe<String> probe = src.runWith(TestSink.<String>probe(system), mat);
        probe.request(10);
        probe.expectNext("b");
        return probe.expectComplete();
      });
    }};
  }

  @Test
  public void test03_StartEventsByTagQuery() {
    new TestKit(system) {{
      Source<String, NotUsed> src = queries
          .eventsByTag("a", Offset.noOffset())
          .map(EventEnvelope::persistenceId);

      awaitAssert(() -> {
        TestSubscriber.Probe<String> probe = src.runWith(TestSink.<String>probe(system), mat);
        probe.request(10);
        probe.expectNext("a");
        probe.expectNoMessage(Duration.ofMillis(100));
        return probe.cancel();
      });
    }};
  }

  @Test
  public void test04_startCurrentEventsByTagQuery() {
    new TestKit(system) {{
      Source<String, NotUsed> src = queries
          .currentEventsByTag("a", Offset.noOffset())
          .map(EventEnvelope::persistenceId);

      awaitAssert(() -> {
        TestSubscriber.Probe<String> probe = src.runWith(TestSink.<String>probe(system), mat);
        probe.request(10);
        probe.expectNext("a");
        return probe.expectComplete();
      });
    }};
  }
}
