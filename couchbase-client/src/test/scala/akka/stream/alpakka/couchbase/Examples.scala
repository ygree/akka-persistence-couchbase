/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase

import akka.{Done, NotUsed}
import akka.stream.Materializer
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.{PersistTo, ReplicateTo}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * Compile only samples for the docs
 */
class Examples {

  implicit val mat: Materializer = null
  implicit val ec: ExecutionContext = null
  val session: CouchbaseSession = null

  def writeSettings(): Unit = {
    //#write-settings
    val couchbaseWriteSettings = CouchbaseWriteSettings(3, ReplicateTo.THREE, PersistTo.FOUR, 2.seconds)
    //#write-settings

  }

  def insertSingleDoc(): Unit = {
    // #by-single-id-flow
    val obj = JsonDocument.create("First", JsonObject.create().put("field", "First"))

    val result: Future[JsonDocument] = session.insert(obj)
    // #by-single-id-flow
  }

  def get(): Unit = {
    //#init-sourceBulk
    val ids = List("First", "Second", "Third", "Fourth")

    val result: Future[immutable.Seq[JsonDocument]] =
      Source(ids)
        .mapAsync(1)(session.get)
        .collect { case Some(doc) => doc }
        .runWith(Sink.seq)
    //#init-sourceBulk
  }

  def upsertSingleSinkSnippet(): Unit = {
    //#init-SingleSink
    import akka.stream.scaladsl.Source
    import com.couchbase.client.java.document.JsonDocument
    val document: JsonDocument =
      JsonDocument.create("id-1", JsonObject.create().put("field", "First"))
    val done = session.upsert(document)
    //#init-SingleSink
  }

  def upsertBulkSinkSnippet(): Unit = {
    //#init-BulkSink
    import akka.stream.scaladsl.Source
    import com.couchbase.client.java.document.JsonDocument
    val documents: List[JsonDocument] =
      List(JsonDocument.create("id-1", JsonObject.create().put("field", "First")))

    val source: Source[JsonDocument, NotUsed] = Source(documents)

    source.mapAsync(1)(session.upsert).runWith(Sink.ignore)
    //#init-BulkSink
  }

  def deleteSingleSinkSnippet(): Unit = {
    //#delete-SingleSink
    import akka.NotUsed
    import akka.stream.scaladsl.Source
    val documentId: String = "id-1"
    val source: Source[String, NotUsed] = Source.single(documentId)
    val done: Future[Done] = session.remove(documentId)

    //#delete-SingleSink
  }

  def deleteBulkSinkSnippet(): Unit = {
    //#delete-BulkSink
    import akka.NotUsed
    import akka.stream.scaladsl.Source

    val documentIds = List("id1", "id2")
    val source: Source[String, NotUsed] = Source(documentIds)

    val deleteSink =
      Flow[String]
        .mapAsync(1)(session.remove)
        .to(Sink.ignore)

    source.runWith(deleteSink)
    //#delete-BulkSink
  }

  def deleteBulkFlowSnippet(): Unit = {

    //#delete-BulkFlow
    import akka.stream.scaladsl.Source

    val documentIds = List("id1", "id2")
    val source: Source[String, NotUsed] = Source(documentIds)

    val deleteFlow: Flow[String, String, NotUsed] =
      Flow[String].mapAsync(1)(
        id => session.remove(id).map(_ => id)
      )

    source
      .via(deleteFlow)
      .runWith(Sink.ignore)
    //#delete-BulkFlow
  }

  def upsertSingleFlowSnippet(): Unit = {
    //#init-SingleFlow
    import akka.stream.scaladsl.Source
    import com.couchbase.client.java.document.JsonDocument
    val document: JsonDocument = JsonDocument.create("id-1", JsonObject.create().put("field", "First"))

    val done: Future[JsonDocument] = session.upsert(document)
    //#init-SingleFlow
  }

}
