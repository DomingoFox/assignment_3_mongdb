#!/bin/bash

set -e

echo "🧹 Stopping and removing containers..."
docker rm -f config-svr-1 config-svr-2 config-svr-3 || true
docker rm -f shard-X-node-a shard-X-node-b shard-X-node-c || true
docker rm -f shard-Y-node-a shard-Y-node-b shard-Y-node-c || true
docker rm -f shard-Z-node-a shard-Z-node-b shard-Z-node-c || true
docker rm -f router-1 router-2 || true

echo "🧹 Removing Docker volumes..."
docker volume prune -f

echo "🧹 Removing Docker network..."
docker network rm mongo-shard-cluster || true

echo "✅ Cleanup complete!"