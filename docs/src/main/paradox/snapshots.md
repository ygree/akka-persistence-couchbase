# Snapshot plugin

The snapshot plugin enables storing and loading snapshots for persistent actor in Couchbase (through the 
[Snapshot store plugin API](https://doc.akka.io/docs/akka/current/persistence-journals.html#snapshot-store-plugin-api).


## Configuration 
For setting up the project to use the plugin, and preparing the couchbase bucket, see [Getting Started](getting-started.md),
especially note the required index.

The snapshot plugin can then be enabled with the following configuration

```hocon
akka.persistence.snapshot-store.plugin = "couchbase-journal.snapshot"
```

The connection settings are shared with the journal plugin, see [Journal Plugin](journal.md) for details.

See [reference.conf](https://github.com/akka/akka-persistence-couchbase/blob/master/core/src/main/resources/reference.conf) 
for complete configuration option docs and defaults.

## Usage

The snapshot plugin is used whenever a snapshot write is triggered through the 
[Akka Persistence APIs](https://doc.akka.io/docs/akka/current/persistence.html#snapshots)
 
