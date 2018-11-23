/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.persistence.query.Offset
import akka.stream.ActorAttributes
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.lightbend.lagom.internal.persistence.couchbase.{CouchbaseOffsetDao, CouchbaseOffsetStore}
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence._
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[couchbase] object CouchbaseReadSideHandler {
  import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseAction

  type Handler[Event] = EventStreamElement[_ <: Event] => Future[immutable.Seq[CouchbaseAction]]

  def emptyHandler[Event]: Handler[Event] =
    _ => Future.successful(immutable.Seq.empty[CouchbaseAction])
}

/**
 * Internal API
 */
private[couchbase] final class CouchbaseReadSideHandler[Event <: AggregateEvent[Event]](
    couchbase: CouchbaseSession,
    offsetStore: CouchbaseOffsetStore,
    handlers: Map[Class[_ <: Event], CouchbaseReadSideHandler.Handler[Event]],
    readProcessorId: String,
    dispatcher: String
)(implicit ec: ExecutionContext)
    extends ReadSideHandler[Event] {

  import CouchbaseReadSideHandler.Handler
  import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseAction

  private val log = LoggerFactory.getLogger(this.getClass)

  @volatile
  private var offsetDao: CouchbaseOffsetDao = _

  protected def invoke(handler: Handler[Event],
                       element: EventStreamElement[Event]): Future[immutable.Seq[CouchbaseAction]] =
    for {
      statements <- handler.apply(element)
    } yield statements :+ offsetDao.bindSaveOffset(element.offset)

  override def prepare(tag: AggregateEventTag[Event]): Future[Offset] =
    for {
      dao <- offsetStore.prepare(readProcessorId, tag.tag)
    } yield {
      offsetDao = dao
      dao.loadedOffset
    }

  override def handle(): Flow[EventStreamElement[Event], Done, NotUsed] = {

    def executeStatements(statements: Seq[CouchbaseAction]): Future[Done] =
      Future.traverse(statements)(a => a.execute(couchbase, ec)).map(_ => Done)

    Flow[EventStreamElement[Event]]
      .mapAsync(parallelism = 1) { elem =>
        val eventClass = elem.event.getClass

        val handler =
          handlers.getOrElse(
            // lookup handler
            eventClass,
            // fallback to empty handler if none
            {
              if (log.isDebugEnabled()) log.debug("Unhandled event [{}]", eventClass.getName)
              CouchbaseReadSideHandler.emptyHandler.asInstanceOf[Handler[Event]]
            }
          )

        invoke(handler, elem).flatMap(executeStatements)

      }
      .withAttributes(ActorAttributes.dispatcher(dispatcher))
  }
}
