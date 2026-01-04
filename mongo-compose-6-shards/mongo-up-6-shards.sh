#!/bin/bash

set -e  # Exit on error

echo "=== Creating Config Server ==="
docker exec configsvr mongosh --port 27020 --eval '
rs.initiate({
  _id: "ConfigRS",
  configsvr: true,
  members: [
    { _id: 0, host: "configsvr:27020" }
  ]
})
'

echo "Waiting for config server to be ready..."
sleep 5

docker exec configsvr mongosh --port 27020 --eval 'rs.status()'

echo ""
echo "=== Creating SHARD1 ==="
docker exec shard1a mongosh --port 27031 --eval '
rs.initiate({
  _id: "sh1",
  members: [
    { _id: 0, host: "shard1a:27031" },
    { _id: 1, host: "shard1b:27032" }
  ]
})
'

echo "Waiting for shard1 to be ready..."
sleep 5

docker exec shard1a mongosh --port 27031 --eval 'rs.status()'

echo ""
echo "=== Creating SHARD2 ==="
docker exec shard2a mongosh --port 27033 --eval '
rs.initiate({
  _id: "sh2",
  members: [
    { _id: 0, host: "shard2a:27033" },
    { _id: 1, host: "shard2b:27034" }
  ]
})
'

echo "Waiting for shard2 to be ready..."
sleep 5

docker exec shard2a mongosh --port 27033 --eval 'rs.status()'

echo ""
echo "=== Creating SHARD3 ==="
docker exec shard3a mongosh --port 27035 --eval '
rs.initiate({
  _id: "sh3",
  members: [
    { _id: 0, host: "shard3a:27035" },
    { _id: 1, host: "shard3b:27036" }
  ]
})
'

echo "Waiting for shard3 to be ready..."
sleep 5

docker exec shard3a mongosh --port 27035 --eval 'rs.status()'

echo ""
echo "=== Creating SHARD4 ==="
docker exec shard4a mongosh --port 27037 --eval '
rs.initiate({
  _id: "sh4",
  members: [
    { _id: 0, host: "shard4a:27037" },
    { _id: 1, host: "shard4b:27038" }
  ]
})
'

echo "Waiting for shard4 to be ready..."
sleep 5

docker exec shard4a mongosh --port 27037 --eval 'rs.status()'

echo ""
echo "=== Creating SHARD5 ==="
docker exec shard5a mongosh --port 27039 --eval '
rs.initiate({
  _id: "sh5",
  members: [
    { _id: 0, host: "shard5a:27039" },
    { _id: 1, host: "shard5b:27040" }
  ]
})
'

echo "Waiting for shard5 to be ready..."
sleep 5

docker exec shard5a mongosh --port 27039 --eval 'rs.status()'

echo ""
echo "=== Creating SHARD6 ==="
docker exec shard6a mongosh --port 27041 --eval '
rs.initiate({
  _id: "sh6",
  members: [
    { _id: 0, host: "shard6a:27041" },
    { _id: 1, host: "shard6b:27042" }
  ]
})
'

echo "Waiting for shard6 to be ready..."
sleep 5

docker exec shard6a mongosh --port 27041 --eval 'rs.status()'

echo ""
echo "=== Configuring Router and Adding Shards ==="
docker exec mongos mongosh --port 27019 --eval '
sh.addShard("sh1/shard1a:27031,shard1b:27032");
sh.addShard("sh2/shard2a:27033,shard2b:27034");
sh.addShard("sh3/shard3a:27035,shard3b:27036");
sh.addShard("sh4/shard4a:27037,shard4b:27038");
sh.addShard("sh5/shard5a:27039,shard5b:27040");
sh.addShard("sh6/shard6a:27041,shard6b:27042");
'

echo "Waiting for shards to be added..."
sleep 5

docker exec mongos mongosh --port 27019 --eval 'sh.status()'

echo ""
echo "=== Enabling Sharding on Database and Collection ==="
docker exec mongos mongosh --port 27019 --eval '
sh.enableSharding("BDABD");
'

docker exec mongos mongosh --port 27019 --eval '
use BDABD;
db.createCollection("taxi_events");
db.taxi_events.createIndex({ "pickup.location.borough_id": "hashed" });
'

docker exec mongos mongosh --port 27019 --eval '
sh.shardCollection("BDABD.taxi_events", { "pickup.location.borough_id": "hashed" });
'

echo ""
echo "=== Shard Distribution ==="
docker exec mongos mongosh --port 27019 --eval '
use BDABD;
db.taxi_events.getShardDistribution();
'

echo ""
echo "=== Setup Complete ==="