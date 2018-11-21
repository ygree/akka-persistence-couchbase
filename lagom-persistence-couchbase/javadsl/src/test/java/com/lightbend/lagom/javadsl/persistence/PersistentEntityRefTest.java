/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.pattern.AskTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.query.N1qlQuery;
import com.lightbend.lagom.javadsl.persistence.PersistentEntity.InvalidCommandException;
import com.lightbend.lagom.javadsl.persistence.PersistentEntity.UnhandledCommandException;
import com.lightbend.lagom.javadsl.persistence.TestEntity.Cmd;
import com.lightbend.lagom.javadsl.persistence.TestEntity.Evt;
import com.lightbend.lagom.javadsl.persistence.TestEntity.State;
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbasePersistenceSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import play.Application;
import play.inject.Injector;
import play.inject.guice.GuiceApplicationBuilder;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static com.lightbend.lagom.internal.persistence.testkit.AwaitPersistenceInit.awaitPersistenceInit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class PersistentEntityRefTest {

  static Injector injector;
  static Application application;

  static CouchbaseCluster couchbaseCluster;
  static Bucket bucket;

  @BeforeClass
  public static void setup() {
    Config config = ConfigFactory.parseString(
        "akka.actor.provider = akka.cluster.ClusterActorRefProvider \n" +
        "akka.remote.netty.tcp.port = 0 \n" +
        "akka.remote.netty.tcp.hostname = 127.0.0.1 \n" +
        "akka.loglevel = INFO \n" +
        "akka.cluster.sharding.distributed-data.durable.keys = [] \n"
        ).withFallback(CouchbasePersistenceSpec.couchbaseConfig());

    application = new GuiceApplicationBuilder()
            .configure(config)
            .build();
    injector = application.injector();

    ActorSystem system = injector.instanceOf(ActorSystem.class);

    Cluster.get(system).join(Cluster.get(system).selfAddress());

    awaitPersistenceInit(system);

    // TODO reuse parts of akka.persistence.couchbase.CouchbaseBucketSetup
    couchbaseCluster = CouchbaseCluster.create()
        .authenticate("admin", "admin1");
    bucket = couchbaseCluster.openBucket("akka");
    bucket.bucketManager().createN1qlPrimaryIndex(true, false);
    bucket.query(N1qlQuery.simple("delete from akka"));
    bucket.bucketManager().dropN1qlPrimaryIndex(true);
  }

  @AfterClass
  public static void teardown() {
    try {
      application.asScala().stop();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    try {
      bucket.close();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    try {
      couchbaseCluster.disconnect();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public static class AnotherEntity extends PersistentEntity<Integer, String, String> {
    @Override
    public Behavior initialBehavior(Optional<String> snapshotState) {
      return newBehavior("");
    }
  }

  private PersistentEntityRegistry registry() {
    PersistentEntityRegistry reg = injector.instanceOf(PersistentEntityRegistry.class);
    reg.register(TestEntity.class);
    return reg;
  }

  @Test
  public void testSendCommandsToTargetEntity() throws Exception {
    PersistentEntityRef<Cmd> ref1 = registry().refFor(TestEntity.class, "1");
    Evt reply1 = ref1.ask(TestEntity.Add.of("a")).toCompletableFuture().get(15, SECONDS);
    assertEquals(new TestEntity.Appended("1", "A"), reply1);

    PersistentEntityRef<Cmd> ref2 = registry().refFor(TestEntity.class, "2");
    Evt reply2 = ref2.ask(TestEntity.Add.of("b")).toCompletableFuture().get(5, SECONDS);
    assertEquals(new TestEntity.Appended("2", "B"), reply2);

    Evt reply3 = ref2.ask(TestEntity.Add.of("c")).toCompletableFuture().get(5, SECONDS);
    assertEquals(new TestEntity.Appended("2", "C"), reply3);

    State state1 = ref1.ask(TestEntity.Get.instance()).toCompletableFuture().get(5, SECONDS);
    assertEquals(Arrays.asList("A"), state1.getElements());

    State state2 = ref2.ask(TestEntity.Get.instance()).toCompletableFuture().get(5, SECONDS);
    assertEquals(Arrays.asList("B", "C"), state2.getElements());
  }

  @Test(expected = AskTimeoutException.class)
  public void testAskTimeout() throws Throwable {
    PersistentEntityRef<Cmd> ref = registry().refFor(TestEntity.class, "10").withAskTimeout(
        FiniteDuration.create(1, MILLISECONDS));

    List<CompletionStage<Evt>> replies = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      replies.add(ref.ask(TestEntity.Add.of("c")));
    }

    for (CompletionStage<Evt> reply : replies) {
      try {
        reply.toCompletableFuture().get(20, SECONDS);
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  @Test(expected = InvalidCommandException.class)
  public void testInvalidCommand() throws Throwable {
    PersistentEntityRef<Cmd> ref = registry().refFor(TestEntity.class, "10");

    try {
      // empty not allowed
      ref.ask(TestEntity.Add.of("")).toCompletableFuture().get(5, SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = NullPointerException.class)
  public void testThrowException() throws Throwable {
    PersistentEntityRef<Cmd> ref = registry().refFor(TestEntity.class, "10");

    try {
      // null will trigger NPE
      ref.ask(TestEntity.Add.of(null)).toCompletableFuture().get(5, SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = UnhandledCommandException.class)
  public void testUnhandledCommand() throws Throwable {
    PersistentEntityRef<Cmd> ref = registry().refFor(TestEntity.class, "10");

    try {
      // empty not allowed
      ref.ask(new TestEntity.UndefinedCmd()).toCompletableFuture().get(5, SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnregistered() throws Throwable {
    registry().refFor(AnotherEntity.class, "1");
  }

}
