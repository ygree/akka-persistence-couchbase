# Developing the plugins

Please feel free to contribute to the Akka Persistence Couchbase plugin by reporting issues you identify, 
or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka-persistence-couchbase/blob/master/CONTRIBUTING.md) to learn how it can be done.

## 

## Running Dockerized Couchbase for tests

Build and start a single node Couchbase cluster in docker:
```bash
docker-compose -f docker/couchbase-1-node/docker-compose.yml up --build
```

Stopping the Couchbase docker container by:
```bash
docker-compose -f docker/couchbase-1-node/docker-compose.yml down
```

Couchbase dashboard is available at `http://localhost:8091` with username `admin` password `admin1`.

## Multi node cluster for tests

This requires access to the separate cluster container nodes, which does work on Linux but not on MacOS or Windows.

Starting a three node Couchbase cluster in docker:

```bash
docker-compose -f docker/couchbase-3-node/docker-compose.yml up --build
```

Stopping the Couchbase docker container by:
```bash
docker-compose -f docker/couchbase-3-node/docker-compose.yml down
```


Check that there are no errors, open up the console at http://localhost:8091 and see that the three server 
nodes are added and the rebalance between them completed (takes quite a while even with an empty bucket).

Note the three IP-addresses logged, update the `application.conf` used for the tests you want to run, for example
`core/src/test/resources/application.conf`, and list the three IPs as `couchbase-journal.connection.nodes`.

run the tests

*Issues stopping the cluster* 

If the docker machines fail to stop with a warning _Cannot kill container 12hash34: unknown error after kill_, 
kill it with fire `sudo killall containerd-shim`