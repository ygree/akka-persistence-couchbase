/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventTag, EventStreamElement}

import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.Done
import akka.persistence.query.{NoOffset, Offset}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

/**
 * Couchbase read side support.
 *
 * This should be used to build and register a read side processor.
 */
object CouchbaseReadSide {

  /**
   * Builder for the handler.
   */
  trait ReadSideHandlerBuilder[Event <: AggregateEvent[Event]] {

    /**
     * Set a global prepare callback.
     *
     * @param callback The callback.
     * @return This builder for fluent invocation.
     * @see ReadSideHandler#globalPrepare()
     */
    def setGlobalPrepare(callback: CouchbaseSession => Future[Done]): ReadSideHandlerBuilder[Event]

    /**
     * Set a prepare callback.
     *
     * @param callback The callback.
     * @return This builder for fluent invocation.
     * @see ReadSideHandler#prepare(AggregateEventTag)
     */
    def setPrepare(
        callback: (CouchbaseSession, AggregateEventTag[Event]) => Future[Done]
    ): ReadSideHandlerBuilder[Event]

    /**
     * Define the event handler that will be used for events of a given class.
     *
     * @param handler The function to handle the events.
     * @tparam E The event type to handle.
     * @return This builder for fluent invocation
     */
    def setEventHandler[E <: Event: ClassTag](
        handler: (CouchbaseSession, EventStreamElement[E]) => Future[Done]
    ): ReadSideHandlerBuilder[Event]

    /**
     * Build the read side handler.
     *
     * @return The read side handler.
     */
    def build(): ReadSideHandler[Event]
  }

}

trait CouchbaseReadSide {

  /**
   * Create a builder for a Cassandra read side event handler.
   *
   * @param readSideId An identifier for this read side. This will be used to store offsets in the offset store.
   * @return The builder.
   */
  def builder[Event <: AggregateEvent[Event]](readSideId: String): CouchbaseReadSide.ReadSideHandlerBuilder[Event]
}
