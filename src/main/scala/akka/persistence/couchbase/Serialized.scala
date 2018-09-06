package akka.persistence.couchbase

import java.util.Base64

import akka.persistence.couchbase.CouchbaseJournal.Fields
import akka.serialization.{Serialization, Serializers}
import com.couchbase.client.java.document.json.JsonObject

case class Serialized(identifier: Int, manifest: String, payload: Array[Byte]) {
  def asJson(): JsonObject = {
    JsonObject.create()
      .put(Fields.SerializerManifest, manifest)
      .put(Fields.SerializerId, identifier)
      .put(Fields.Payload, Base64.getEncoder.encodeToString(payload))
  }
}

object Serialized {
  // TODO AsycSerializers

  def serialize(serialization: Serialization, event: AnyRef): Serialized = {
    val serializer = serialization.findSerializerFor(event)
    val serManifest = Serializers.manifestFor(serializer, event)
    val serId: Int = serializer.identifier
    val bytes = serialization.serialize(event).get // TODO deal with failure
    Serialized(serId, serManifest, bytes)
  }

  def fromJsonObject(serialization: Serialization, jsonObject: JsonObject): Any = {
    val serId = jsonObject.getInt(Fields.SerializerId)
    val serManifest = jsonObject.getString(Fields.SerializerManifest)
    val payload = jsonObject.getString(Fields.Payload)
    serialization.deserialize(Base64.getDecoder.decode(payload), serId, serManifest).get
  }

}
