# Query plugin

poc in place, fill out doc section when more ready

```scala
val queries = PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)
```

The connection settings are shared with the read journal, see above. Some read journal specific settings
are available under `couchbase-journal.read` (see [reference.conf](https://github.com/akka/akka-persistence-couchbase/blob/master/core/src/main/resources/reference.conf) 
for setting docs and defaults)

## Events by tag and eventual consistency

The event by tag stream is globally sorted using a time based UUID, this means it is very important that the Akka
nodes are kept in tight clock sync. Even with clocks in sync, writing to different nodes of the couchbase cluster
could mean that the change reaches the query index out of order, to protect against missing events because of this
or small clock skew the queries will never read the latest events but only up to a point in time a little while ago
giving better chance that the index is consistent with the written data.

The offset back in time is configured through the setting `couchbase-journal.read.events-by-tag.eventual-consistency-delay`.
By default it is set to 5 seconds, this means that at any time a query is performed it will only see events up until
5 seconds ago. For the live query it will keep catching up to 5 seconds ago as time passes by. 

## Caveats

 * *Important* Tagged events may be missed in the events by tag query (We will address this in a later milestone, 
   tracked by [#97](https://github.com/akka/akka-persistence-couchbase/issues/97))
   * if there is clock skew on the Akka nodes that is larger than the `couchbase-journal.read.events-by-tag.eventual-consistency-delay`
   * if `couchbase-journal.read.events-by-tag.eventual-consistency-delay` is tuned so low that the couchbase cluster
     hasn't reached consistency at that time
 * As the indexes used to perform the queries are eventually consistent (even for a single writer node) there 
   is no guarantee that an immediate query will see the latest writes.
 