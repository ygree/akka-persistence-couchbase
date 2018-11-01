/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.actor.ActorSystem
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

import scala.concurrent.ExecutionContext

/**
 * Internal API
 */
private[lagom] object CouchbaseReadSideSessionProvider {

  // TODO decide what we want to expose, probably not a raw AsyncBucket
  def apply(system: ActorSystem, executionContext: ExecutionContext): CouchbaseSession = {

    ???
  }
}
