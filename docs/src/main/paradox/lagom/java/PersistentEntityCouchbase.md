# Storing Persistent Entities in Couchbase

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

It's highly recommended that each service use a unique bucket name so that the documents of different services do not conflict with each other.
You need to configure the connections and buckets that are used for these documents in each of your service implementation projects.

Lagom has three internal components that require connection and bucket configuration:

* The **journal** stores serialized events
* The **snapshot store** stores snapshots of the state as an optimization for faster recovery 
(see [Snapshots|PersistentEntity#Snapshots]] for details)
* The **offset store** is used for [[Couchbase Read-Side support|ReadSideCouchbase]] to keep track of the most recent 
event handled by each read-side processor (detailed in [[Read-side design|ReadSide#Read-side-design]]).

You can configure the connections and bucket names in each service implementation project's `application.conf` file:

```conf
couchbase-journal {
  write.bucket = my_service_journal
  snapshot.bucket = my_service_snapshot

  connection {
    nodes = ["localhost"]
    username = "admin"
    password = "admin1"
  }
}

lagom.persistence.read-side.couchbase {
  bucket = my_service_read_side

  connection {
    nodes = ["localhost"]
    username = "admin"
    password = "admin1"
  }
}
```

While different services should be isolated by using different buckets, it is perfectly fine to use the same bucket 
for all of these components within one service. In that case, it can be convenient to define a custom bucket configuration 
property and use [property substitution](https://github.com/typesafehub/config#factor-out-common-values) to avoid repeating it.

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

Lagom's Couchbase support is provided by the [`akka-persistence-couchbase`](../../index.html) plugin. 
A full configuration reference can be in the plugin's [`reference.conf`](https://github.com/akka/akka-persistence-couchbase/blob/master/core/src/main/resources/reference.conf).

## Couchbase Location

The application should be configured to connect to the existing Couchbase cluster.

@@@ note
Lagom will NOT start Couchbase service automatically. 
@@@


@@@ note { title=Hint }
The Couchbase Docker container can be used for development. For more details see [Developing the plugin](../../developing.html)
@@@


@@@ note
Couchbase connection can ONLY be configured statically. Dynamically locatable Couchbase server is not supported at the moment. 
@@@
