/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.actor.ActorSystem
import com.couchbase.client.java.AsyncBucket
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore

import scala.concurrent.ExecutionContext

/**
 * Internal API
 */
private[lagom] final class ScaladslCouchbaseOffsetStore(system: ActorSystem, session: AsyncBucket,
                                                        config: ReadSideConfig)(implicit ec: ExecutionContext)
  extends CouchbaseOffsetStore(system, config, session)
