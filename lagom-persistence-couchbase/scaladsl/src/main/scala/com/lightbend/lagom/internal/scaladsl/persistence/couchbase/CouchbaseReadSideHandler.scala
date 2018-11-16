/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.persistence.couchbase.AsyncCouchbaseSession
import akka.persistence.query.Offset
import akka.stream.ActorAttributes
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.lightbend.lagom.internal.persistence.couchbase.{CouchbaseAction, CouchbaseOffsetDao, CouchbaseOffsetStore}
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence._
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API
 */
private[couchbase] abstract class CouchbaseReadSideHandler[Event <: AggregateEvent[Event], Handler](
    couchbase: CouchbaseSession,
    handlers: Map[Class[_ <: Event], Handler],
    dispatcher: String
)(implicit ec: ExecutionContext)
    extends ReadSideHandler[Event] {

  private val log = LoggerFactory.getLogger(this.getClass)

  protected def invoke(handler: Handler, event: EventStreamElement[Event]): Future[immutable.Seq[CouchbaseAction]]

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
              CouchbaseAutoReadSideHandler.emptyHandler.asInstanceOf[Handler]
            }
          )

        invoke(handler, elem).flatMap(executeStatements)

      }
      .withAttributes(ActorAttributes.dispatcher(dispatcher))
  }
}

/**
 * Internal API
 */
private[couchbase] object CouchbaseAutoReadSideHandler {

  type Handler[Event] = EventStreamElement[_ <: Event] => Future[immutable.Seq[CouchbaseAction]]

  def emptyHandler[Event]: Handler[Event] =
    _ => Future.successful(immutable.Seq.empty[CouchbaseAction])
}

/**
 * Internal API
 */
private[couchbase] final class CouchbaseAutoReadSideHandler[Event <: AggregateEvent[Event]](
    couchbase: CouchbaseSession,
    offsetStore: CouchbaseOffsetStore,
    handlers: Map[Class[_ <: Event], CouchbaseAutoReadSideHandler.Handler[Event]],
    readProcessorId: String,
    dispatcher: String
)(implicit ec: ExecutionContext)
    extends CouchbaseReadSideHandler[Event, CouchbaseAutoReadSideHandler.Handler[Event]](couchbase,
                                                                                         handlers,
                                                                                         dispatcher) {

  import CouchbaseAutoReadSideHandler.Handler

  @volatile
  private var offsetDao: CouchbaseOffsetDao = _

  override protected def invoke(handler: Handler[Event],
                                element: EventStreamElement[Event]): Future[immutable.Seq[CouchbaseAction]] =
    for {
      statements <- handler
        .asInstanceOf[EventStreamElement[Event] => Future[immutable.Seq[CouchbaseAction]]]
        .apply(element)
    } yield statements :+ offsetDao.bindSaveOffset(element.offset)

  protected def offsetStatement(offset: Offset): immutable.Seq[CouchbaseAction] =
    immutable.Seq(offsetDao.bindSaveOffset(offset))

  override def prepare(tag: AggregateEventTag[Event]): Future[Offset] =
    for {
      dao <- offsetStore.prepare(readProcessorId, tag.tag)
    } yield {
      offsetDao = dao
      dao.loadedOffset
    }
}
