# Journal plugin

## Features

 * All operations required by the [Akka Persistence journal plugin API](https://doc.akka.io/docs/akka/current/scala/persistence.html#journal-plugin-api) are fully supported.
 * The plugin uses Couchbase in a mostly log-oriented way i.e. data are only ever inserted but never updated 
   (deletions are made on user request only). The exception is event metadata on deletion.
 * Writes of messages are batched to optimize throughput for persistAsync. See [batch writes](https://doc.akka.io/docs/akka/current/scala/persistence.html#batch-writes) for details how to configure batch sizes. 


## Configuration

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


