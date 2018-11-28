/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.javadsl.persistence.couchbase

import akka.actor.ActorSystem
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore
import javax.inject.{Inject, Singleton}

/**
 * Internal API
 */
@Singleton
private[lagom] final class JavadslCouchbaseOffsetStore @Inject()(system: ActorSystem,
                                                                 couchbase: CouchbaseSession,
                                                                 config: ReadSideConfig)
    extends CouchbaseOffsetStore(system, config, couchbase.asScala)
