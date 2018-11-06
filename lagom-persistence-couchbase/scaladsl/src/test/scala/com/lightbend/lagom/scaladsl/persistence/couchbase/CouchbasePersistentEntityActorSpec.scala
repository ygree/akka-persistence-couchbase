/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import com.lightbend.lagom.scaladsl.persistence.{AbstractPersistentEntityActorSpec, TestEntitySerializerRegistry}

class CouchbasePersistentEntityActorSpec
    extends CouchbasePersistenceSpec(TestEntitySerializerRegistry)
    with AbstractPersistentEntityActorSpec
