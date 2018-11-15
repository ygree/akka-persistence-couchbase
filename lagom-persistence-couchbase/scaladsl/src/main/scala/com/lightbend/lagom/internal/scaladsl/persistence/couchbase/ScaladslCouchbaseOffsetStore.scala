/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.persistence.couchbase

import akka.actor.ActorSystem
import akka.persistence.couchbase.Couchbase
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseOffsetStore

/**
 * Internal API
 */
private[lagom] final class ScaladslCouchbaseOffsetStore(system: ActorSystem,
                                                        couchbase: Couchbase,
                                                        config: ReadSideConfig)
    extends CouchbaseOffsetStore(system, config, couchbase)
