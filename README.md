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
CREATE INDEX `tags` ON `akka`((all (`all_tags`)),`ordering`);
```

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

The connection settings are shared with the read journal, see above


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