#!/bin/bash

set -m

USERNAME=admin
PASSWORD=admin1
CLUSTER_NAME=akka
BUCKET=akka

# Variable used in echo
# Echo with
log() {
  echo "[$(date +"%T")] $@"
}

# Run the server and send it to the background
/entrypoint.sh couchbase-server &

log "waiting for http://localhost:8091/ui/index.html"
while [ "$(curl -Isw '%{http_code}' -o /dev/null http://localhost:8091/ui/index.html#/)" != 200 ]
do
    sleep 5
done

echo "Type: $TYPE"

if [ "$TYPE" = "MASTER" ]; then
  couchbase-cli cluster-init -c 127.0.0.1 --cluster-username $USERNAME --cluster-password $PASSWORD \
    --cluster-name $CLUSTER_NAME --cluster-ramsize 256 --cluster-index-ramsize 256 --services data,index,query,fts \
    --index-storage-setting default

  # Create the bucket
  log "Create buckets ........."
  couchbase-cli bucket-create -c 127.0.0.1 --username $USERNAME --password $PASSWORD --bucket-type couchbase \
    --bucket-ramsize 100 --bucket $BUCKET --enable-flush 1

  # Need to wait until query service is ready to process N1QL queries
  log "Waiting for master to be ready for queries"
  sleep 15 #TODO: how to check if it's ready to process N1QL queries

  # Create indexes
  log "Create bucket indices ........."
  cbq -u $USERNAME -p $PASSWORD -s "CREATE INDEX \`pi2\` ON \`akka\`((self.\`persistence_id\`),(self.\`sequence_from\`));"
  cbq -u $USERNAME -p $PASSWORD -s "CREATE INDEX \`tags\` ON \`akka\`((all (\`all_tags\`)),\`ordering\`);"

  # rebalance once the other two nodes are added
  log "Sleeping to give time for replicas to join"
  sleep 30 #TODO check that workers joined instead
  log "Rebalance triggered"
  couchbase-cli rebalance -c 127.0.0.1 --user=$USERNAME --password=$PASSWORD

fi
if [ "$TYPE" = "WORKER" ]; then
  log "Sleeping so that master has created bucket and indexes"
  sleep 30 #TODO: how to check that master has created indexes instead

  #IP=`hostname -s`
  IP=`hostname -I | cut -d ' ' -f1`
  log "Joining cluster, my IP: " $IP
  couchbase-cli server-add --cluster=$COUCHBASE_MASTER:8091 --user=$USERNAME --password=$PASSWORD --server-add=$IP --server-add-username=$USERNAME --server-add-password=$PASSWORD
fi;



fg 1
