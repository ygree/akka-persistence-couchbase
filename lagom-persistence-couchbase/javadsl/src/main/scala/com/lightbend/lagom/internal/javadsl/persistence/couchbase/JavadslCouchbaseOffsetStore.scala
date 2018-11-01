/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import javax.inject.{ Inject, Singleton }
import akka.actor.ActorSystem
import com.couchbase.client.java.AsyncBucket
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore

import scala.concurrent.ExecutionContext

/**
 * Internal API
 */
@Singleton
private[lagom] final class JavadslCouchbaseOffsetStore @Inject() (
  system:  ActorSystem,
  session: AsyncBucket,
  config:  ReadSideConfig)(implicit ec: ExecutionContext)
  extends CouchbaseOffsetStore(system, config, session)
