/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import CouchbaseReadSideHandler.Handler
import akka.Done
import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseReadSide
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseReadSide.ReadSideHandlerBuilder
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventTag, EventStreamElement}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Internal API
 */
private[lagom] final class CouchbaseReadSideImpl(system: ActorSystem,
                                                 couchbaseSession: CouchbaseSession,
                                                 offsetStore: CouchbaseOffsetStore)
    extends CouchbaseReadSide {

  private val dispatcher = system.settings.config.getString("lagom.persistence.read-side.use-dispatcher")
  implicit val ec: MessageDispatcher = system.dispatchers.lookup(dispatcher)

  override def builder[Event <: AggregateEvent[Event]](readSideId: String): ReadSideHandlerBuilder[Event] =
    new ReadSideHandlerBuilder[Event] {
      var globalPrepare: CouchbaseSession => Future[Done] =
        (_) => Future.successful(Done)

      var prepare: (CouchbaseSession, AggregateEventTag[Event]) => Future[Done] =
        (_, _) => Future.successful(Done)

      private var handlers = Map.empty[Class[_ <: Event], Handler[Event]]

      override def setGlobalPrepare(callback: CouchbaseSession => Future[Done]): ReadSideHandlerBuilder[Event] = {
        globalPrepare = callback
        this
      }

      override def setPrepare(
          callback: (CouchbaseSession, AggregateEventTag[Event]) => Future[Done]
      ): ReadSideHandlerBuilder[Event] = {
        prepare = callback
        this
      }

      override def setEventHandler[E <: Event: ClassTag](
          handler: (CouchbaseSession, EventStreamElement[E]) => Future[Done]
      ): ReadSideHandlerBuilder[Event] = {
        val eventClass = implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[Event]]
        handlers += (eventClass -> handler.asInstanceOf[Handler[Event]])
        this
      }

      override def build(): ReadSideHandler[Event] =
        new CouchbaseReadSideHandler[Event](couchbaseSession,
                                            offsetStore,
                                            handlers,
                                            globalPrepare,
                                            prepare,
                                            readSideId,
                                            dispatcher)
    }
}
