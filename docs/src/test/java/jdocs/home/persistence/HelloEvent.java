/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package jdocs.home.persistence;

import com.lightbend.lagom.javadsl.persistence.AggregateEvent;
import com.lightbend.lagom.javadsl.persistence.AggregateEventShards;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;

public interface HelloEvent extends AggregateEvent<HelloEvent> {

  int NUM_SHARDS = 20;

  AggregateEventShards<HelloEvent> TAG = AggregateEventTag.sharded(HelloEvent.class, NUM_SHARDS);

  @Override
  default AggregateEventShards<HelloEvent> aggregateTag() {
    return TAG;
  }

  class GreetingMessageChanged implements HelloEvent {
    public final String name;
    public final String message;

    public GreetingMessageChanged(String name, String message) {
      this.name = name;
      this.message = message;
    }
  }
}
