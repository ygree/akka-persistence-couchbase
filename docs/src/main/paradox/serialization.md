# Serialization

The couchbase journal uses the Akka serialization infrastructure to serialize events and snapshots into bytes
which are then Base64 encoded and put inside the documents as strings. This is convenient as it allows re-use of exisiting
serializer plugins for Akka but has some overhead because of the string encoded binary data (somewhere around 1/3 on top
of the raw data size).

It is possible to serialize to JSON that is readable directly from the database by registering a serializer that extends
`akka.persistence.couchbase.JsonSerializer`. This makes the event and snapshots in the database more transparent but
comes with the caveat that it is mutually exclusive with the commercial Lightbend GDPR support.

