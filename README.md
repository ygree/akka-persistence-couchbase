# Couchbase Plugins for Akka Persistence

Replicated [Akka Persistence](https://doc.akka.io/docs/akka/current/scala/persistence.html) journal and snapshot 
store for [Couchbase](https://www.couchbase.com).

Note that this project is in early access, see section below for details about what is more or less complete and
what is in progress.

For questions please use the [discuss.akka.io](https://discuss.akka.io). Tag any new questions with `akka-persistence` and `couchbase`.

[![Build Status](https://travis-ci.org/akka/akka-persistence-couchbase.svg?branch=master)](https://travis-ci.org/akka/akka-persistence-couchbase)

## Project status

The plugins are available in an early access mode, we will publish milestones as we work towards a 1.0 release.

Current state:

*Write Journal* implemented with minor details in progress, should work but not production ready
*Snapshot Store* POC implementation in place, not hardened, expect changes
*Read Journal* POC implementation in place, not hardened, expect changes
*Lagom support* POC implementation in place, not hardened, expect changes

## Dependencies

To include the latest release of the Couchbase plugins for **Akka 2.5.x** into your sbt project, add the following lines to your build file:

*sbt build.sbt*
```scala
libraryDependencies += "com.lightbend.akka" %% "akka-persistence-couchbase" % "0.1"
```

*Maven pom.xml*
```xml
<dependency>
  <groupId>com.lightbend.akka</groupId>
  <artifactId>akka-persistence-couchbase_2.12</artifactId>
  <version>0.1</version>
</depencency>
```

This version of akka-persistence-couchbase depends on Akka 2.5.15. It has been published for Scala 2.11 and 2.12. 

## Journal plugin

### Features

 * All operations required by the [Akka Persistence journal plugin API](https://doc.akka.io/docs/akka/current/scala/persistence.html#journal-plugin-api) are fully supported.
 * The plugin uses Couchbase in a mostly log-oriented way i.e. data are only ever inserted but never updated 
   (deletions are made on user request only). The exception is event metadata on deletion.
 * Writes of messages are batched to optimize throughput for persistAsync. See [batch writes](https://doc.akka.io/docs/akka/current/scala/persistence.html#batch-writes) for details how to configure batch sizes. 


### Configuration

Enable one or more of the plugins in `application.conf` and configure the cluster connection details:

```hocon
akka.persistence.journal.plugin = "couchbase-journal.write"

couchbase-journal {
  connection {
    nodes = ["192.168.0.2", "192.168.0.3", "192.168.0.4"] # if left empty defaults to [ "localhost" ]
    username = "scott"
    password = "tiger"
  }
}
```

For more settings see [refrence.conf](https://github.com/akka/akka-persistence-couchbase/blob/master/core/src/main/resources/reference.conf) 

### Preconditions

You will also need to create a bucket, by default called `akka` and the indexes below.  

**Required indexes**

The following global secondary indexes needs to be created for the plugins to function:

The journal requires the index

```
CREATE INDEX `pi2` ON `akka`((self.`persistence_id`),(self.`sequence_from`));
```

If you will be using the query side with event-for-tags the following will also be required:

```
CREATE INDEX `tags` ON `akka` 
  (ALL ARRAY m.tags FOR m IN messages END)
CREATE INDEX `tags-ordering` ON `akka` 
  (DISTINCT ARRAY m.ordering FOR m IN messages END)
```

## Serialization

The couchbase journal uses the Akka serialization infrastructure to serialize events and snapshots into bytes
which are then Base64 encoded and put inside the documents as strings. This is convenient as it allows re-use of exisiting
serializer plugins for Akka but has some overhead because of the string encoded binary data (somewhere around 1/3 on top
of the raw data size).

It is possible to serialize to JSON that is readable directly from the database by registering a serializer that extends
`akka.persistence.couchbase.JsonSerializer`. This makes the event and snapshots in the database more transparent but
comes with the caveat that it is mutually exclusive with the commercial Lightbend GDPR support.


### Caveats

 * In preview status - not production ready
 * Performance and failure scenario testing has not yet been done
 * Smaller schema changes will likely happen and there will not be any migration tools provided as long as
   project releases are milestones

## Snapshot plugin

poc in place, fill out doc section when more ready

```hocon
akka.persistence.snapshot-store.plugin = "couchbase-journal.snapshot"
```

The connection settings are shared with the read journal, see above


## Query plugin

poc in place, fill out doc section when more ready

```scala
val queries = PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)
```

The connection settings are shared with the read journal, see above. Some read journal specific settings
are available under `couchbase-journal.read` (see [reference.conf](blob/master/core/src/main/resources/reference.conf) 
for setting docs and defaults)

### Events by tag and eventual consistency

The event by tag stream is globally sorted using a time based UUID, this means it is very important that the Akka
nodes are kept in tight clock sync. Even with clocks in sync, writing to different nodes of the couchbase cluster
could mean that the change reaches the query index out of order, to protect against missing events because of this
or small clock skew the queries will never read the latest events but only up to a point in time a little while ago
giving better chance that the index is consistent with the written data.

The offset back in time is configured through the setting `couchbase-journal.read.events-by-tag.eventual-consistency-delay`.
By default it is set to 5 seconds, this means that at any time a query is performed it will only see events up until
5 seconds ago. For the live query it will keep catching up to 5 seconds ago as time passes by. 

### Caveats

 * *Important* Tagged events may be missed in the events by tag query (We will address this in a later milestone, 
   tracked by [#97](https://github.com/akka/akka-persistence-couchbase/issues/97))
   * if there is clock skew on the Akka nodes that is larger than the `couchbase-journal.read.events-by-tag.eventual-consistency-delay`
   * if `couchbase-journal.read.events-by-tag.eventual-consistency-delay` is tuned so low that the couchbase cluster
     hasn't reached consistency at that time
 * As the indexes used to perform the queries are eventually consistent (even for a single writer node) there 
   is no guarantee that an immediate query will see the latest writes.
 


## Developing the plugins

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution details.

### Running Dockerized Couchbase for tests

Build and start a single node Couchbase cluster in docker:
```
docker-compose -f docker/couchbase-1-node/docker-compose.yml build && \ 
  docker-compose -f docker/couchbase-1-node/docker-compose.yml up
```

Stopping the Couchbase docker container by:
```
docker-compose -f docker/couchbase-1-node/docker-compose.yml down
```

Couchbase dashboard is available at `http://localhost:8091` with username `admin` password `admin1`.

### Multi node cluster for tests

This requires access to the separate cluster container nodes, which does work on Linux but not on MacOS or Windows.

Starting the nodes:

```
docker-compose -f docker/couchbase-3-node/docker-compose.yml build && \
  docker-compose -f docker/couchbase-3-node/docker-compose.yml up
```


Check that there are no errors, open up the console at http://localhost:8091 and see that the three server 
nodes are added and the rebalance between them completed (takes quite a while even with an empty bucket).

Note the three IP-addresses logged, update the `application.conf` used for the tests you want to run, for example
`core/src/test/resources/application.conf`, and list the three IPs as `couchbase-journal.connection.nodes`.

run the tests

*Issues stopping the cluster* 

If the docker machines fail to stop with a warning _Cannot kill container 12hash34: unknown error after kill_, 
kill it with fire `sudo killall containerd-shim`


## License

Akka Persistence Couchbase is Open Source and available under the Apache 2 License.