# Snapshot plugin

poc in place, fill out doc section when more ready

```hocon
akka.persistence.snapshot-store.plugin = "couchbase-journal.snapshot"
```

The connection settings are shared with the read journal, see above