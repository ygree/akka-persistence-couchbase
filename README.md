# Akka Persistence Couchbase

Expects a local Couchbase with username `admin` password `admin1` and a 
bucket called `akka`.

Running couchbase with docker:

```
docker run -d --name db -p 8091-8094:8091-8094 -p 11210:11210 couchbase  
```

Then go to: `http://localhost:8091` and create a bucket called `akka`

Index:

For recovery and events by persistenceId:
```
CREATE INDEX `pi2` ON `akka`((self.`persistence_id`),(self.`sequence_from`))
```

For events by tag:
```
CREATE INDEX `tags` ON `akka`((all (`all_tags`)),`ordering`)
```

