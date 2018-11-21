#!/usr/bin/env bash

cd test
echo "===> Changing directory to \"./test\""

docker-compose up -d --build --force-recreate cassandra kafka mongo
rc=$?
if [[ $rc != 0 ]]
  then exit $rc
fi

function ping_cassandra() {
  docker exec -it cassandra /usr/bin/nodetool status | grep UN
  res=$?
}

echo "Waiting for Cassandra to be ready."

# For some reason, GoCql still can't connect to Cassandra even if the nodetool
# shows positive results. There has to be a better way than this.
max_attempts=40
cur_attempts=0
ping_cassandra
while (( res != 0 && ++cur_attempts != max_attempts ))
do
  ping_cassandra
  echo Attempt: $cur_attempts of $max_attempts
  sleep 1
done

if (( cur_attempts == max_attempts )); then
  echo "Cassandra Timed Out."
  exit 1
else
  echo "Cassandra response received."
fi

# The Cassandra image takes more time to be ready despite
# nodetool-status being success.
# There has to be a better way than this.
echo "Waiting additional time for Cassandra to be ready."
add_wait=30
cur_add_wait=0
while (( ++cur_add_wait != add_wait ))
do
  echo Additional Wait: $cur_add_wait of $add_wait seconds
  sleep 1
done

docker-compose up -d --build --force-recreate go-eventpersistence
rc=$?
if [[ $rc != 0 ]]
  then exit $rc
fi
echo "Waiting for go-eventpersistence to initialize"
sleep 10

docker-compose up -d --build --force-recreate go-eventstore-query
rc=$?
if [[ $rc != 0 ]]
  then exit $rc
fi
echo "Waiting for go-eventstore-query to initialize"
sleep 10

docker ps -a

docker-compose up --exit-code-from go-agg-builder
exit $?
