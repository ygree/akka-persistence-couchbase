/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.stream.alpakka.couchbase.scaladsl.Couchbase
import com.lightbend.lagom.internal.persistence.couchbase.{CouchbaseAction, CouchbaseOffsetStore}
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseReadSide
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseReadSide.ReadSideHandlerBuilder
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, EventStreamElement}

import scala.collection.immutable
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Internal API
 */
private[lagom] final class CouchbaseReadSideImpl(system: ActorSystem,
                                                 couchbase: Couchbase,
                                                 offsetStore: CouchbaseOffsetStore)
    extends CouchbaseReadSide {

  private val dispatcher = system.settings.config.getString("lagom.persistence.read-side.use-dispatcher")
  implicit val ec: MessageDispatcher = system.dispatchers.lookup(dispatcher)

  override def builder[Event <: AggregateEvent[Event]](eventProcessorId: String): ReadSideHandlerBuilder[Event] =
    new ReadSideHandlerBuilder[Event] {
      import CouchbaseAutoReadSideHandler.Handler
      private var handlers = Map.empty[Class[_ <: Event], Handler[Event]]

      override def setEventHandler[E <: Event: ClassTag](
          handler: EventStreamElement[E] => Future[immutable.Seq[CouchbaseAction]]
      ): ReadSideHandlerBuilder[Event] = {
        val eventClass = implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[Event]]
        handlers += (eventClass -> handler.asInstanceOf[Handler[Event]])
        this
      }

      override def build(): ReadSideHandler[Event] =
        new CouchbaseAutoReadSideHandler[Event](couchbase, offsetStore, handlers, eventProcessorId, dispatcher)
    }
}
