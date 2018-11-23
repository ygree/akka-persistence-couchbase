/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase;

import com.lightbend.lagom.javadsl.persistence.couchbase.JavaDslCouchbaseAction;
import com.lightbend.lagom.javadsl.persistence.AggregateEvent;
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Couchbase read side support.
 * <p>
 * This should be used to build and register a read side processor.
 */
public interface CouchbaseReadSide {

  /**
   * Create a builder for a Couchbase read side event handler.
   *
   * @param readSideId An identifier for this read side. This will be used to store offsets in the offset store.
   * @return The builder.
   */
  <Event extends AggregateEvent<Event>> ReadSideHandlerBuilder<Event> builder(String readSideId);


  /**
   * Builder for the handler.
   */
  interface ReadSideHandlerBuilder<Event extends AggregateEvent<Event>> {
    /**
     * Define the event handler that will be used for events of a given class.
     *
     * @param eventClass The event class to handle.
     * @param handler    The function to handle the events.
     * @return This builder for fluent invocation
     */
    <E extends Event> ReadSideHandlerBuilder<Event> setEventHandler(Class<E> eventClass, Function<E, CompletionStage<List<JavaDslCouchbaseAction>>> handler);

    /**
     * Build the read side handler.
     *
     * @return The read side handler.
     */
    ReadSideProcessor.ReadSideHandler<Event> build();
  }
}

