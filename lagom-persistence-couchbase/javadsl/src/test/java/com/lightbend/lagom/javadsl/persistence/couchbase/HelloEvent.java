/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = HelloEvent.GreetingMessageChanged.class, name = "greeting-message-changed")
})
public interface HelloEvent {

  String getName();

  final class GreetingMessageChanged implements HelloEvent {

    String name;
    String message;

    @JsonCreator
    public GreetingMessageChanged(@JsonProperty("name") String name, @JsonProperty("message") String message) {
      this.name = Preconditions.checkNotNull(name, "name");
      this.message = Preconditions.checkNotNull(message, "message");
    }

    @Override
    public String getName() {
      return name;
    }

    public String getMessage() {
      return message;
    }
  }
}
