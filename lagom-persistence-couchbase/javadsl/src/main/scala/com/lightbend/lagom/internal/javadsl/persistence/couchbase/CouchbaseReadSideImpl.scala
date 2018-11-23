/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase
import java.util.concurrent.CompletionStage
import java.util.function.{Function => JFunction}
import java.util.{List => JList}

import akka.actor.ActorSystem
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import com.lightbend.lagom.internal.javadsl.persistence.ReadSideImpl
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore
import com.lightbend.lagom.javadsl.persistence.{AggregateEvent, Offset, ReadSideProcessor}
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbaseReadSide
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbaseReadSide.ReadSideHandlerBuilder
import javax.inject.{Inject, Singleton}
import play.api.inject.Injector

@Singleton
private[lagom] class CouchbaseReadSideImpl @Inject()(
    system: ActorSystem,
    couchbaseSession: CouchbaseSession,
    offsetStore: CouchbaseOffsetStore,
    injector: Injector
) extends CouchbaseReadSide {
  import akka.dispatch.MessageDispatcher

  private val dispatcher = system.settings.config.getString("lagom.persistence.read-side.use-dispatcher")
  implicit val ec: MessageDispatcher = system.dispatchers.lookup(dispatcher)

  override def builder[Event <: AggregateEvent[Event]](readSideId: String): ReadSideHandlerBuilder[Event] =
    new ReadSideHandlerBuilder[Event] {
      import com.lightbend.lagom.javadsl.persistence.couchbase.JavaDslCouchbaseAction
      type Handler[E] = CouchbaseReadSideHandler.Handler[E]
      private var handlers = Map.empty[Class[_ <: Event], Handler[Event]]

      override def setEventHandler[E <: Event](
          eventClass: Class[E],
          handler: JFunction[E, CompletionStage[JList[JavaDslCouchbaseAction]]]
      ): ReadSideHandlerBuilder[Event] = {

        handlers += (eventClass -> ((event: E, offset: Offset) => handler(event)))
        this
      }

      override def build(): ReadSideProcessor.ReadSideHandler[Event] =
        new CouchbaseReadSideHandler[Event](couchbaseSession, offsetStore, handlers, readSideId, dispatcher)
    }
}
