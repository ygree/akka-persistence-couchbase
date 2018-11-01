/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.actor.ActorSystem
import com.couchbase.client.java.AsyncBucket

import scala.concurrent.ExecutionContext

/**
 * Internal API
 */
private[lagom] object CouchbaseReadSideSessionProvider {

  // TODO decide what we want to expose, probably not a raw AsyncBucket
  def apply(system: ActorSystem, executionContext: ExecutionContext): AsyncBucket = {

    ???
  }
}
