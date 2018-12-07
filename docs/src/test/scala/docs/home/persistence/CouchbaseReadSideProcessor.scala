/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package docs.home.persistence

// #imports
import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseReadSide
import com.lightbend.lagom.scaladsl.persistence.{AggregateEventTag, EventStreamElement, ReadSide, ReadSideProcessor}

import scala.concurrent.{ExecutionContext, Future}

// #imports

object CouchbaseReadSideProcessorInitial {
  //#initial
  class HelloEventProcessor extends ReadSideProcessor[HelloEvent] {

    override def buildHandler(): ReadSideProcessor.ReadSideHandler[HelloEvent] =
      // TODO build read side handler
      ???

    override def aggregateTags: Set[AggregateEventTag[HelloEvent]] =
      // TODO return the tag for the events
      ???
  }
  //#initial
}

object CouchbaseReadSideProcessorTwo {

  class HelloEventProcessor(readSide: ReadSide with CouchbaseReadSide)(implicit executionContext: ExecutionContext)
      extends ReadSideProcessor[HelloEvent] {

    //#tag
    override def aggregateTags: Set[AggregateEventTag[HelloEvent]] = HelloEvent.Tag.allTags
    //#tag

    // #create-document
    val DocId = "users-actual-greetings"

    private def createDocument(session: CouchbaseSession): Future[Done] =
      session.get(DocId).flatMap {
        case Some(doc) => Future.successful(Done)
        case None =>
          session
            .upsert(JsonDocument.create(DocId, JsonObject.empty()))
            .map(_ => Done)
      }

    // #create-document

    // #prepare-statements
    private def prepare(session: CouchbaseSession, tag: AggregateEventTag[HelloEvent]): Future[Done] =
      // TODO do something when read-side is run for each shard
      Future.successful(Done)
    // #prepare-statements

    override def buildHandler(): ReadSideProcessor.ReadSideHandler[HelloEvent] = {
      // #create-builder
      val builder = readSide.builder[HelloEvent]("all-greetings")
      // #create-builder

      // #register-global-prepare
      builder.setGlobalPrepare(createDocument _)
      // #register-global-prepare

      // #set-event-handler
      builder.setEventHandler[HelloEvent.GreetingChanged](processGreetingMessageChanged _)
      // #set-event-handler

      // #register-prepare
      builder.setPrepare(prepare _)
      // #register-prepare

      // #build
      builder.build()
      // #build
    }

    // #greeting-message-changed
    def processGreetingMessageChanged(session: CouchbaseSession,
                                      ese: EventStreamElement[HelloEvent.GreetingChanged]): Future[Done] =
      session
        .get(DocId)
        .flatMap { maybeDoc =>
          val json = maybeDoc match {
            case Some(doc) => doc.content()
            case None => JsonObject.create();
          }
          val evt = ese.event
          json.put(evt.name, evt.message)
          session.upsert(JsonDocument.create(DocId, json))
        }
        .map(_ => Done)
    // #greeting-message-changed

  }
}
