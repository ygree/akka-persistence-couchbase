/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CouchbaseSessionTest {

  static CouchbaseCluster couchbaseCluster;
  static Bucket bucket;
  static CouchbaseSession session;

  static final String bucketName = "couchbaseSessionTest";

  @BeforeClass
  public static void setup() {
    couchbaseCluster = CouchbaseCluster.create()
        .authenticate("admin", "admin1");

    if (!couchbaseCluster.clusterManager().hasBucket(bucketName)) {
      BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
          .type(BucketType.COUCHBASE)
          .name(bucketName)
          .quota(100)
          .build();
      couchbaseCluster.clusterManager().insertBucket(bucketSettings);
    }

    bucket = couchbaseCluster.openBucket(bucketName);
    bucket.bucketManager().createN1qlPrimaryIndex(true, false);
    bucket.query(N1qlQuery.simple("delete from " + bucketName));
    bucket.bucketManager().dropN1qlPrimaryIndex(true);

    session = CouchbaseSession.create(bucket);
  }

  @AfterClass
  public static void teardown() {
    try {
      session.close();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    try {
      bucket.close();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    try {
      couchbaseCluster.clusterManager().removeBucket(bucketName);
    } catch (Throwable t) {
      t.printStackTrace();
    }
    try {
      couchbaseCluster.disconnect();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Test
  public void testInsertAndRead() throws ExecutionException, InterruptedException {
    JsonObject insertObject = JsonObject.create();
    insertObject.put("intVal", 1);

    JsonDocument inserted = session.insert(JsonDocument.create("one", insertObject)).toCompletableFuture().get();
    assertEquals("one", inserted.id());
    assertEquals(new Integer(1), inserted.content().getInt("intVal"));


    Optional<JsonDocument> getResult = session.get("one").toCompletableFuture().get();
    assertTrue(getResult.isPresent());
    JsonDocument getDoc = getResult.get();
    assertEquals("one", getDoc.id());
    assertEquals(new Integer(1), getDoc.content().getInt("intVal"));
  }


}
