# Akka Persistence Couchbase

## Required indexes

The journal requires the index

```
CREATE INDEX `pi2` ON `akka`((self.`persistence_id`),(self.`sequence_from`));
```

and using the query side with event-for-tags requires

```
CREATE INDEX `tags` ON `akka`((all (`all_tags`)),`ordering`);
```

During tests these are automatically created but for running an actual system you will have to manually create them.

## Running Dockerized Couchbase for tests

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

## Multi node cluster for tests

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