#!/bin/bash

set -e

echo "✅ Creating Docker network..."
docker network create mongo-shard-cluster || echo "⚠️  Network already exists, skipping."

echo "✅ Starting Config Server containers..."
docker run -d --net mongo-shard-cluster --name config-svr-1 -p 27101:27017 mongo:4.4 mongod --port 27017 --configsvr --replSet config-svr-replica-set
docker run -d --net mongo-shard-cluster --name config-svr-2 -p 27102:27017 mongo:4.4 mongod --port 27017 --configsvr --replSet config-svr-replica-set
docker run -d --net mongo-shard-cluster --name config-svr-3 -p 27103:27017 mongo:4.4 mongod --port 27017 --configsvr --replSet config-svr-replica-set

echo "⏳ Waiting for config servers to initialize..."
sleep 10

echo "✅ Initiating Config Server Replica Set..."
docker exec config-svr-1 mongo --eval '
rs.initiate({
  _id: "config-svr-replica-set",
  configsvr: true,
  members: [
    { _id: 0, host: "config-svr-1:27017" },
    { _id: 1, host: "config-svr-2:27017" },
    { _id: 2, host: "config-svr-3:27017" }
  ]
})'

sleep 10

echo "✅ Starting Shard X replica set containers..."
docker run -d --net mongo-shard-cluster --name shard-X-node-a -p 27110:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-X-replica-set
docker run -d --net mongo-shard-cluster --name shard-X-node-b -p 27111:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-X-replica-set
docker run -d --net mongo-shard-cluster --name shard-X-node-c -p 27112:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-X-replica-set

echo "✅ Starting Shard Y replica set containers..."
docker run -d --net mongo-shard-cluster --name shard-Y-node-a -p 27113:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-Y-replica-set
docker run -d --net mongo-shard-cluster --name shard-Y-node-b -p 27114:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-Y-replica-set
docker run -d --net mongo-shard-cluster --name shard-Y-node-c -p 27115:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-Y-replica-set

echo "✅ Starting Shard Z replica set containers..."
docker run -d --net mongo-shard-cluster --name shard-Z-node-a -p 27116:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-Z-replica-set
docker run -d --net mongo-shard-cluster --name shard-Z-node-b -p 27117:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-Z-replica-set
docker run -d --net mongo-shard-cluster --name shard-Z-node-c -p 27118:27017 mongo:4.4 mongod --port 27017 --shardsvr --replSet shard-Z-replica-set

echo "⏳ Waiting for shard nodes to initialize..."
sleep 10

echo "✅ Initiating Shard X Replica Set..."
docker exec shard-X-node-a mongo --eval '
rs.initiate({
  _id: "shard-X-replica-set",
  members: [
    { _id: 0, host: "shard-X-node-a:27017" },
    { _id: 1, host: "shard-X-node-b:27017" },
    { _id: 2, host: "shard-X-node-c:27017" }
  ]
})'

echo "✅ Initiating Shard Y Replica Set..."
docker exec shard-Y-node-a mongo --eval '
rs.initiate({
  _id: "shard-Y-replica-set",
  members: [
    { _id: 0, host: "shard-Y-node-a:27017" },
    { _id: 1, host: "shard-Y-node-b:27017" },
    { _id: 2, host: "shard-Y-node-c:27017" }
  ]
})'

echo "✅ Initiating Shard Z Replica Set..."
docker exec shard-Z-node-a mongo --eval '
rs.initiate({
  _id: "shard-Z-replica-set",
  members: [
    { _id: 0, host: "shard-Z-node-a:27017" },
    { _id: 1, host: "shard-Z-node-b:27017" },
    { _id: 2, host: "shard-Z-node-c:27017" }
  ]
})'

sleep 10

echo "✅ Starting Mongos Routers..."
docker run -d --net mongo-shard-cluster --name router-1 -p 27141:27017 mongo:4.4 mongos --port 27017 --configdb config-svr-replica-set/config-svr-1:27017,config-svr-2:27017,config-svr-3:27017 --bind_ip_all
docker run -d --net mongo-shard-cluster --name router-2 -p 27142:27017 mongo:4.4 mongos --port 27017 --configdb config-svr-replica-set/config-svr-1:27017,config-svr-2:27017,config-svr-3:27017 --bind_ip_all

echo "⏳ Waiting for mongos routers to be ready..."
sleep 10


echo "✅ Adding shards to cluster via router-1..."
docker exec router-1 mongo --eval '
sh.addShard("shard-X-replica-set/shard-X-node-a:27017,shard-X-node-b:27017,shard-X-node-c:27017");
sh.addShard("shard-Y-replica-set/shard-Y-node-a:27017,shard-Y-node-b:27017,shard-Y-node-c:27017");
sh.addShard("shard-Z-replica-set/shard-Z-node-a:27017,shard-Z-node-b:27017,shard-Z-node-c:27017");
'

echo "✅ Enabling sharding for database 'assignment_3'..."
docker exec router-1 mongo --eval '
sh.enableSharding("assignment_3")
'

echo "✅ Sharding 'assignment_3.vessel_db' on key { MMSI: "hashed" }..."
docker exec router-1 mongo --eval '
sh.shardCollection("assignment_3.vessel_db", { MMSI: "hashed" })
'
echo "✅ Sharding 'assignment_3.vessel_db' on key { MMSI: "hashed" }..."
docker exec router-1 mongo --eval '
sh.shardCollection("assignment_3.filtered_vessel_db", { MMSI: "hashed" })
'

echo "✅ Ensuring balancer is running..."
docker exec router-1 mongo --eval '
sh.startBalancer()
'

echo "✅ MongoDB sharded cluster setup complete!"