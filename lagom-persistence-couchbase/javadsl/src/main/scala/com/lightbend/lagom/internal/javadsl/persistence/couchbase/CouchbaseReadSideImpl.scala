/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import java.util.concurrent.CompletionStage
import java.util.function.BiFunction

import akka.Done
import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbaseReadSide
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbaseReadSide.ReadSideHandlerBuilder
import com.lightbend.lagom.javadsl.persistence.{AggregateEvent, Offset, ReadSideProcessor}
import javax.inject.{Inject, Singleton}
import play.api.inject.Injector

@Singleton
private[lagom] class CouchbaseReadSideImpl @Inject()(
    system: ActorSystem,
    couchbaseSession: CouchbaseSession,
    offsetStore: CouchbaseOffsetStore,
    injector: Injector
) extends CouchbaseReadSide {

  private val dispatcher = system.settings.config.getString("lagom.persistence.read-side.use-dispatcher")
  private implicit val ec: MessageDispatcher = system.dispatchers.lookup(dispatcher)

  override def builder[Event <: AggregateEvent[Event]](readSideId: String): ReadSideHandlerBuilder[Event] =
    new ReadSideHandlerBuilder[Event] {
      type Handler[E] = CouchbaseReadSideHandler.Handler[E]
      private var handlers = Map.empty[Class[_ <: Event], Handler[Event]]

      override def setEventHandler[E <: Event](
          eventClass: Class[E],
          handler: BiFunction[CouchbaseSession, E, CompletionStage[Done]]
      ): ReadSideHandlerBuilder[Event] = {

        handlers += (eventClass -> ((cs: CouchbaseSession, event: E, offset: Offset) => handler(cs, event)))
        this
      }

      override def build(): ReadSideProcessor.ReadSideHandler[Event] =
        new CouchbaseReadSideHandler[Event](couchbaseSession, offsetStore, handlers, readSideId, dispatcher)
    }
}
