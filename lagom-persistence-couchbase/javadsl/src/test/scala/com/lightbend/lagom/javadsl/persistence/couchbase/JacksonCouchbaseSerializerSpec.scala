/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import akka.actor.ExtendedActorSystem
import com.lightbend.lagom.persistence.ActorSystemSpec

class JacksonCouchbaseSerializerSpec extends ActorSystemSpec {

  val jacksonSerializer = new JacksonCouchbaseSerializer(system.asInstanceOf[ExtendedActorSystem])

  "JacksonCouchbaseSerializer" should {

    "serialize and deserialize" in {
      val evt = new HelloEvent.GreetingMessageChanged("Alice", "Hello")
      val serialized = jacksonSerializer.toBinary(evt)

      val value = jacksonSerializer.fromBinary(serialized, jacksonSerializer.manifest(evt))
      val result = value.asInstanceOf[HelloEvent.GreetingMessageChanged]

      result.message should ===(evt.message)
      result.name should ===(evt.name)
    }
  }

}
