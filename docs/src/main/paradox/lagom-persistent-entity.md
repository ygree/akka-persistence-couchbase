# Lagom: Persistent Entities

This page describes how to configure Couchbase for use with Lagom's 
[Persistent Entity](https://www.lagomframework.com/documentation/1.4.x/java/PersistentEntity.html) API.

## Project dependencies

To use Couchbase add the following in your project's build:

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=lagom-javadsl-persistence-couchbase_$scala.binary.version$
  version=$project.version$
}


## Configuration

Each service **must** use a unique bucket name so that the documents of different services do not conflict with each other.
You need to configure the connections and buckets that are used for these documents in each of your service implementation projects.

Lagom has three internal components that require connection and bucket configuration:

* The **journal** stores serialized events
* The **snapshot store** stores snapshots of the state as an optimization for faster recovery 
* The **offset store** is used for [Read-Side support](lagom-read-side.md) to keep track of the most recent 
event handled by each read-side processor.

While different services should be isolated by using different buckets, it is perfectly fine to use the same bucket 
for all of these components within one service. In that case, it can be convenient to define a custom bucket configuration 
property and use [property substitution](https://github.com/typesafehub/config#factor-out-common-values) to avoid repeating it.

You can configure the connections and bucket names in each service implementation project's `application.conf` file:
```conf
my-service.couchbase {
  bucket = "akka"

  connection {
    nodes = ["localhost"]
    username = "admin"
    password = "admin1"
  }
}

couchbase-journal {
  write.bucket = ${my-service.couchbase.bucket}
  snapshot.bucket = ${my-service.couchbase.bucket}

  connection = ${my-service.couchbase.connection}
}

lagom.persistence.read-side.couchbase {
  bucket = ${my-service.couchbase.bucket}

  connection = ${my-service.couchbase.connection}
}
```

It is also possible to use a separate config for each of the plugins, like so: 

```conf
couchbase-journal {
  write.bucket = my_service_journal
  snapshot.bucket = my_service_snapshot

  connection {
    nodes = ["cluster-b"]
    username = "admin"
    password = "admin1"
  }
}

lagom.persistence.read-side.couchbase {
  bucket = my_service_read_side

  connection {
    nodes = ["cluster-b"]
    username = "admin"
    password = "admin1"
  }
}
```



Lagom's Couchbase support is provided by the [`akka-persistence-couchbase`](index.md) plugin. 
A full configuration reference can be in the plugin's [`reference.conf`](https://github.com/akka/akka-persistence-couchbase/blob/master/core/src/main/resources/reference.conf).

## Couchbase Location

The application should be configured to connect to the existing Couchbase cluster.

## Caveats

 * Lagom will **NOT** start Couchbase service automatically. 
    * The Couchbase Docker container can be used for development. For more details see [Developing the plugin](developing.md)
 * Couchbase connection can ONLY be configured statically. Dynamically locatable Couchbase server is not supported at the moment. 
   [Tracked by issue #135](https://github.com/akka/akka-persistence-couchbase/issues/135)
