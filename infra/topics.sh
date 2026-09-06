#!/bin/bash
set -e

echo "Waiting for Redpanda to be ready..."
sleep 5

echo "Creating topic: raw-trades"
docker exec redpanda rpk topic create raw-trades \
  --partitions 3 \
  --replicas 1

echo "Creating topic: raw-klines"
docker exec redpanda rpk topic create raw-klines \
  --partitions 3 \
  --replicas 1

echo "Creating topic: raw-bookticker"
docker exec redpanda rpk topic create raw-bookticker \
  --partitions 3 \
  --replicas 1

echo "Listing all topics:"
docker exec redpanda rpk topic list