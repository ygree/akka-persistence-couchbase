/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import java.util.concurrent.CompletionStage

import akka.Done
import akka.japi.Pair
import akka.stream.ActorAttributes
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import akka.stream.javadsl.Flow
import com.lightbend.lagom.internal.javadsl.persistence.OffsetAdapter
import com.lightbend.lagom.internal.persistence.couchbase.{CouchbaseOffsetDao, CouchbaseOffsetStore}
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.javadsl.persistence.{AggregateEvent, AggregateEventTag, Offset}
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[couchbase] object CouchbaseReadSideHandler {
  type Handler[Event] = (CouchbaseSession, _ <: Event, Offset) => CompletionStage[Done]

  def emptyHandler[Event, E <: Event]: Handler[Event] =
    (_: CouchbaseSession, _: E, _: Offset) => Future.successful(Done.getInstance()).toJava
}

import CouchbaseReadSideHandler.Handler

/**
 * Internal API
 */
private[couchbase] final class CouchbaseReadSideHandler[Event <: AggregateEvent[Event]](
    couchbaseSession: CouchbaseSession,
    offsetStore: CouchbaseOffsetStore,
    handlers: Map[Class[_ <: Event], Handler[Event]],
    globalPrepareCallback: CouchbaseSession => CompletionStage[Done],
    prepareCallback: (CouchbaseSession, AggregateEventTag[Event]) => CompletionStage[Done],
    readProcessorId: String,
    dispatcher: String
)(implicit ec: ExecutionContext)
    extends ReadSideHandler[Event] {

  private val log = LoggerFactory.getLogger(this.getClass)

  @volatile
  private var offsetDao: CouchbaseOffsetDao = _

  protected def invoke(handler: Handler[Event], event: Event, offset: Offset): CompletionStage[Done] =
    handler
      .asInstanceOf[(CouchbaseSession, Event, Offset) => CompletionStage[Done]]
      .apply(couchbaseSession, event, offset)
      .toScala
      .flatMap { _ =>
        val akkaOffset = OffsetAdapter.dslOffsetToOffset(offset)
        offsetDao.bindSaveOffset(akkaOffset).execute(couchbaseSession.asScala, ec)
      }
      .toJava

  override def globalPrepare(): CompletionStage[Done] = globalPrepareCallback.apply(couchbaseSession)

  override def prepare(tag: AggregateEventTag[Event]): CompletionStage[Offset] =
    (for {
      _ <- prepareCallback.apply(couchbaseSession, tag).toScala
      dao <- offsetStore.prepare(readProcessorId, tag.tag)
    } yield {
      offsetDao = dao
      OffsetAdapter.offsetToDslOffset(dao.loadedOffset)
    }).toJava

  override def handle(): Flow[Pair[Event, Offset], Done, _] =
    akka.stream.scaladsl
      .Flow[Pair[Event, Offset]]
      .mapAsync(parallelism = 1) { pair =>
        val Pair(event, offset) = pair
        val eventClass = event.getClass

        val handler =
          handlers.getOrElse(
            // lookup handler
            eventClass,
            // fallback to empty handler if none
            {
              if (log.isDebugEnabled()) log.debug("Unhandled event [{}]", eventClass.getName)
              CouchbaseReadSideHandler.emptyHandler
            }
          )

        invoke(handler, event, offset).toScala

      }
      .withAttributes(ActorAttributes.dispatcher(dispatcher))
      .asJava
}
