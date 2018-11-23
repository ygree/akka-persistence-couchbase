/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import java.util
import java.util.concurrent.CompletionStage
import java.util.{List => JList}

import akka.Done
import akka.japi.Pair
import akka.stream.ActorAttributes
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import akka.stream.javadsl.Flow
import com.lightbend.lagom.internal.javadsl.persistence.OffsetAdapter
import com.lightbend.lagom.internal.persistence.couchbase.{CouchbaseAction, CouchbaseOffsetDao, CouchbaseOffsetStore}
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.javadsl.persistence.{AggregateEvent, AggregateEventTag, Offset}
import org.pcollections.TreePVector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[couchbase] object CouchbaseReadSideHandler {
  import com.lightbend.lagom.javadsl.persistence.couchbase.JavaDslCouchbaseAction

  type Handler[Event] = (_ <: Event, Offset) => CompletionStage[JList[JavaDslCouchbaseAction]]

  def emptyHandler[Event, E <: Event]: Handler[Event] =
    (_: E, _: Offset) => Future.successful(util.Collections.emptyList[JavaDslCouchbaseAction]()).toJava
}

import CouchbaseReadSideHandler.Handler

/**
 * Internal API
 */
private[couchbase] final class CouchbaseReadSideHandler[Event <: AggregateEvent[Event]](
    couchbase: CouchbaseSession,
    offsetStore: CouchbaseOffsetStore,
    handlers: Map[Class[_ <: Event], Handler[Event]],
    readProcessorId: String,
    dispatcher: String
)(implicit ec: ExecutionContext)
    extends ReadSideHandler[Event] {

  import com.lightbend.lagom.javadsl.persistence.couchbase.JavaDslCouchbaseAction

  private val log = LoggerFactory.getLogger(this.getClass)

  @volatile
  private var offsetDao: CouchbaseOffsetDao = _

  protected def invoke(handler: Handler[Event],
                       event: Event,
                       offset: Offset): CompletionStage[JList[JavaDslCouchbaseAction]] = {
    val couchbaseActions = for {
      handlerActions <- handler
        .asInstanceOf[(Event, Offset) => CompletionStage[JList[JavaDslCouchbaseAction]]]
        .apply(event, offset)
        .toScala
    } yield {
      val akkaOffset = OffsetAdapter.dslOffsetToOffset(offset)
      TreePVector
        .from(handlerActions)
        .plus(JavaDslCouchbaseAction(offsetDao.bindSaveOffset(akkaOffset)))
        .asInstanceOf[JList[JavaDslCouchbaseAction]]
    }
    couchbaseActions.toJava
  }

  override def prepare(tag: AggregateEventTag[Event]): CompletionStage[Offset] =
    (for {
      dao <- offsetStore.prepare(readProcessorId, tag.tag)
    } yield {
      offsetDao = dao
      OffsetAdapter.offsetToDslOffset(dao.loadedOffset)
    }).toJava

  override def handle(): Flow[Pair[Event, Offset], Done, _] = {

    def executeStatements(statements: JList[JavaDslCouchbaseAction]): Future[Done] =
      Future.traverse(statements.asScala)(a => a.execute(couchbase).toScala).map(_ => Done)

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

        invoke(handler, event, offset).toScala.flatMap(executeStatements)

      }
      .withAttributes(ActorAttributes.dispatcher(dispatcher))
      .asJava
  }
}