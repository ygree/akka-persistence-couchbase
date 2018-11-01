/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import com.lightbend.lagom.scaladsl.persistence.{AbstractPersistentEntityActorSpec, TestEntitySerializerRegistry}


class CouchbasePersistentEntityActorSpec extends CouchbasePersistenceSpec(TestEntitySerializerRegistry) with AbstractPersistentEntityActorSpec
