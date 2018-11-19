/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase
import java.io.NotSerializableException

import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.couchbase.JsonSerializer
import com.couchbase.client.java.document.json.JsonObject
import com.lightbend.lagom.internal.jackson.JacksonObjectMapperProvider
import com.lightbend.lagom.serialization.JacksonJsonMigration

import scala.util.{Failure, Success}

/**
 * Enable by next configuration:
 *
 * akka.actor.serializers.js = "com.lightbend.lagom.javadsl.persistence.couchbase.JacksonCouchbaseSerializer"
 * akka.actor.serialization-bindings {
 *     "com.lightbend.lagom.serialization.Jsonable" = js
 * }
 * akka.actor.serialization-identifiers {
 *     "com.lightbend.lagom.javadsl.persistence.couchbase.JacksonCouchbaseSerializer" = 2002
 * }
 */
//TODO: figure out how ot enable it optionally
class JacksonCouchbaseSerializer(val system: ExtendedActorSystem) extends JsonSerializer {
  private val log = Logging.getLogger(system, getClass)
  private val conf = system.settings.config.getConfig("lagom.serialization.json")
  private val isDebugEnabled = log.isDebugEnabled
  private val objectMapper = JacksonObjectMapperProvider(system).objectMapper
  private val migrations: Map[String, JacksonJsonMigration] = {
    import scala.collection.JavaConverters._
    conf
      .getConfig("migrations")
      .root
      .unwrapped
      .asScala
      .toMap
      .map {
        case (k, v) ⇒
          val transformer = system.dynamicAccess.createInstanceFor[JacksonJsonMigration](v.toString, Nil).get
          k -> transformer
      }(collection.breakOut)
  }

  override def manifest(obj: AnyRef): String = {
    val className = obj.getClass.getName
    migrations.get(className) match {
      case Some(transformer) => className + "#" + transformer.currentVersion
      case None => className
    }
  }

  override def toJson(o: AnyRef): JsonObject = {
    val jsonStr = objectMapper.writeValueAsString(o)
    JsonObject.fromJson(jsonStr)
  }
  override def fromJson(json: JsonObject, manifest: String): AnyRef = {
    val (fromVersion, manifestClassName) = parseManifest(manifest)

    val migration = migrations.get(manifestClassName)

    val className = migration match {
      case Some(transformer) if fromVersion < transformer.currentVersion =>
        transformer.transformClassName(fromVersion, manifestClassName)
      case Some(transformer) if fromVersion > transformer.currentVersion =>
        throw new IllegalStateException(
          s"Migration version ${transformer.currentVersion} is " +
          s"behind version $fromVersion of deserialized type [$manifestClassName]"
        )
      case _ => manifestClassName
    }

    val clazz = system.dynamicAccess.getClassFor[AnyRef](className) match {
      case Success(clazz) ⇒ clazz
      case Failure(e) ⇒
        throw new NotSerializableException(
          s"Cannot find manifest class [$className] for serializer [${getClass.getName}]."
        )
    }

    val jsonStr = json.toString //TODO avoid String cross conversion
    objectMapper.readValue(jsonStr, clazz)
    //    JacksonTransformers.MAPPER.readValue(jsonStr, clazz).asInstanceOf[AnyRef]

  }
  private def parseManifest(manifest: String) = {
    val i = manifest.lastIndexOf('#')
    val fromVersion = if (i == -1) 1 else manifest.substring(i + 1).toInt
    val manifestClassName = if (i == -1) manifest else manifest.substring(0, i)
    (fromVersion, manifestClassName)
  }
  override def identifier: Int = 2002
}
