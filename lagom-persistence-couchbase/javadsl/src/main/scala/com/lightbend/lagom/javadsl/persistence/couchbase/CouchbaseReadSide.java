/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase;

import akka.Done;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
import com.lightbend.lagom.javadsl.persistence.AggregateEvent;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.Offset;
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
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
     * Set a global prepare callback.
     *
     * @param callback The callback.
     * @return This builder for fluent invocation.
     * @see ReadSideProcessor.ReadSideHandler#globalPrepare()
     */
    ReadSideHandlerBuilder<Event> setGlobalPrepare(Function<CouchbaseSession, CompletionStage<Done>> callback);

    /**
     * Set a prepare callback.
     *
     * @param callback The callback.
     * @return This builder for fluent invocation.
     * @see ReadSideProcessor.ReadSideHandler#prepare(AggregateEventTag)
     */
    ReadSideHandlerBuilder<Event> setPrepare(BiFunction<CouchbaseSession, AggregateEventTag<Event>, CompletionStage<Done>> callback);

    /**
     * Define the event handler that will be used for events of a given class.
     *
     * @param eventClass The event class to handle.
     * @param handler    The function to handle the events.
     * @return This builder for fluent invocation
     */
    <E extends Event> ReadSideHandlerBuilder<Event> setEventHandler(Class<E> eventClass, BiFunction<CouchbaseSession, E, CompletionStage<Done>> handler);


    /**
     * Define the event handler that will be used for events of a given class.
     * <p>
     * This variant allows for offsets to be consumed as well as their events.
     *
     * @param eventClass The event class to handle.
     * @param handler    The function to handle the events.
     * @return This builder for fluent invocation
     */
    <E extends Event> ReadSideHandlerBuilder<Event> setEventHandler(Class<E> eventClass, TriConsumer<CouchbaseSession, E, Offset, CompletionStage<Done>> handler);

    /**
     * Build the read side handler.
     *
     * @return The read side handler.
     */
    ReadSideProcessor.ReadSideHandler<Event> build();
  }

  /**
   * SAM for consuming a connection.
   */
  @FunctionalInterface
  public interface TriConsumer<T, U, O, R> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param o the third function argument
     * @return the function result
     */
    R apply(T t, U u, O o);
  }
}

