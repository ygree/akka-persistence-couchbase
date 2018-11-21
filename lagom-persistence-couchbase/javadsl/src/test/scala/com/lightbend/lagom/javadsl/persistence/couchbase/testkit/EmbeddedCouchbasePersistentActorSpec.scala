/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase.testkit
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbasePersistenceSpec
import com.lightbend.lagom.javadsl.persistence.testkit.AbstractEmbeddedPersistentActorSpec

class EmbeddedCouchbasePersistentActorSpec extends CouchbasePersistenceSpec with AbstractEmbeddedPersistentActorSpec
