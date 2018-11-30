/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase.testkit

import com.lightbend.lagom.scaladsl.persistence.TestEntitySerializerRegistry
import com.lightbend.lagom.scaladsl.persistence.testkit.AbstractEmbeddedPersistentActorSpec
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbasePersistenceSpec

class EmbeddedCouchbasePersistentActorSpec
    extends CouchbasePersistenceSpec(TestEntitySerializerRegistry)
    with AbstractEmbeddedPersistentActorSpec
