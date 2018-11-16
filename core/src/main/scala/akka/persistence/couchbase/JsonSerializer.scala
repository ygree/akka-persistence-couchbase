/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase
import akka.serialization.SerializerWithStringManifest
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.transcoder.JacksonTransformers

/**
 * Special serializer base class to use Couchbase native json for serializing the events and snapshots.
 *
 * Subclass, implement and register through the regular Akka serialization infrastructure to use.
 */
abstract class JsonSerializer extends SerializerWithStringManifest {

  def toJson(o: AnyRef): JsonObject
  def fromJson(json: JsonObject, manifest: String): AnyRef

  override def toBinary(o: AnyRef): Array[Byte] = {
    val json = toJson(o)
    val wrapper = JsonObject
      .create()
      .put("payload", json)
      .put("manifest", manifest(o))
    JacksonTransformers.MAPPER.writeValueAsBytes(wrapper)
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val wrapper = JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject])
    fromJson(wrapper.getObject("payload"), wrapper.getString("manifest"))
  }
}
