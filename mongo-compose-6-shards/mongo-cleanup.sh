#!/bin/bash

set -e  # Exit on error

echo "=========================================="
echo "MongoDB Sharded Cluster Cleanup Script (6 Shards)"
echo "=========================================="
echo ""

# Function to safely execute commands
safe_exec() {
    if docker ps -q -f name=$1 2>/dev/null | grep -q .; then
        echo "Processing: $1"
        return 0
    else
        echo "Container $1 not found or not running, skipping..."
        return 1
    fi
}

echo "=== Step 1: Removing Sharded Collections ==="
if safe_exec "mongos"; then
    docker exec mongos mongosh --port 27019 --eval '
    use BDABD;
    db.taxi_events.drop();
    ' 2>/dev/null || echo "Collection already dropped or doesn't exist"
fi

echo ""
echo "=== Step 2: Disabling Sharding on Database ==="
if safe_exec "mongos"; then
    docker exec mongos mongosh --port 27019 --eval '
    use BDABD;
    db.dropDatabase();
    ' 2>/dev/null || echo "Database already dropped or doesn't exist"
fi

echo ""
echo "=== Step 3: Removing Shards from Cluster ==="
if safe_exec "mongos"; then
    echo "Removing shard6..."
    docker exec mongos mongosh --port 27019 --eval '
    db.adminCommand({ removeShard: "sh6" });
    ' 2>/dev/null || echo "Shard sh6 already removed or doesn't exist"

    echo "Removing shard5..."
    docker exec mongos mongosh --port 27019 --eval '
    db.adminCommand({ removeShard: "sh5" });
    ' 2>/dev/null || echo "Shard sh5 already removed or doesn't exist"

    echo "Removing shard4..."
    docker exec mongos mongosh --port 27019 --eval '
    db.adminCommand({ removeShard: "sh4" });
    ' 2>/dev/null || echo "Shard sh4 already removed or doesn't exist"

    echo "Removing shard3..."
    docker exec mongos mongosh --port 27019 --eval '
    db.adminCommand({ removeShard: "sh3" });
    ' 2>/dev/null || echo "Shard sh3 already removed or doesn't exist"

    echo "Removing shard2..."
    docker exec mongos mongosh --port 27019 --eval '
    db.adminCommand({ removeShard: "sh2" });
    ' 2>/dev/null || echo "Shard sh2 already removed or doesn't exist"

    echo "Removing shard1..."
    docker exec mongos mongosh --port 27019 --eval '
    db.adminCommand({ removeShard: "sh1" });
    ' 2>/dev/null || echo "Shard sh1 already removed or doesn't exist"
fi

echo ""
echo "=== Step 4: Stopping Router (mongos) ==="
if docker ps -q -f name=mongos | grep -q .; then
    docker stop mongos 2>/dev/null || true
    docker rm mongos 2>/dev/null || true
    echo "✓ Router stopped and removed"
else
    echo "Router already stopped"
fi

echo ""
echo "=== Step 5: Stopping Config Server ==="
if safe_exec "configsvr"; then
    docker exec configsvr mongosh --port 27020 --eval '
    use admin;
    db.shutdownServer({ force: true });
    ' 2>/dev/null || true
fi

if docker ps -q -f name=configsvr | grep -q .; then
    docker stop configsvr 2>/dev/null || true
    docker rm configsvr 2>/dev/null || true
    echo "✓ Config server stopped and removed"
else
    echo "Config server already stopped"
fi

echo ""
echo "=== Step 6: Stopping Shard 1 Replica Set ==="
for container in shard1a shard1b; do
    if docker ps -q -f name=$container | grep -q .; then
        docker stop $container 2>/dev/null || true
        docker rm $container 2>/dev/null || true
        echo "✓ $container stopped and removed"
    else
        echo "$container already stopped"
    fi
done

echo ""
echo "=== Step 7: Stopping Shard 2 Replica Set ==="
for container in shard2a shard2b; do
    if docker ps -q -f name=$container | grep -q .; then
        docker stop $container 2>/dev/null || true
        docker rm $container 2>/dev/null || true
        echo "✓ $container stopped and removed"
    else
        echo "$container already stopped"
    fi
done

echo ""
echo "=== Step 8: Stopping Shard 3 Replica Set ==="
for container in shard3a shard3b; do
    if docker ps -q -f name=$container | grep -q .; then
        docker stop $container 2>/dev/null || true
        docker rm $container 2>/dev/null || true
        echo "✓ $container stopped and removed"
    else
        echo "$container already stopped"
    fi
done

echo ""
echo "=== Step 9: Stopping Shard 4 Replica Set ==="
for container in shard4a shard4b; do
    if docker ps -q -f name=$container | grep -q .; then
        docker stop $container 2>/dev/null || true
        docker rm $container 2>/dev/null || true
        echo "✓ $container stopped and removed"
    else
        echo "$container already stopped"
    fi
done

echo ""
echo "=== Step 10: Stopping Shard 5 Replica Set ==="
for container in shard5a shard5b; do
    if docker ps -q -f name=$container | grep -q .; then
        docker stop $container 2>/dev/null || true
        docker rm $container 2>/dev/null || true
        echo "✓ $container stopped and removed"
    else
        echo "$container already stopped"
    fi
done

echo ""
echo "=== Step 11: Stopping Shard 6 Replica Set ==="
for container in shard6a shard6b; do
    if docker ps -q -f name=$container | grep -q .; then
        docker stop $container 2>/dev/null || true
        docker rm $container 2>/dev/null || true
        echo "✓ $container stopped and removed"
    else
        echo "$container already stopped"
    fi
done

echo ""
echo "=== Step 12: Removing Docker Volumes (Data Cleanup) ==="
echo "WARNING: This will delete ALL data!"
read -p "Do you want to remove all MongoDB data volumes? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Remove volumes for all 6 shards
    docker volume rm mongo-configsvr-data 2>/dev/null || true
    docker volume rm mongo-shard1a-data 2>/dev/null || true
    docker volume rm mongo-shard1b-data 2>/dev/null || true
    docker volume rm mongo-shard2a-data 2>/dev/null || true
    docker volume rm mongo-shard2b-data 2>/dev/null || true
    docker volume rm mongo-shard3a-data 2>/dev/null || true
    docker volume rm mongo-shard3b-data 2>/dev/null || true
    docker volume rm mongo-shard4a-data 2>/dev/null || true
    docker volume rm mongo-shard4b-data 2>/dev/null || true
    docker volume rm mongo-shard5a-data 2>/dev/null || true
    docker volume rm mongo-shard5b-data 2>/dev/null || true
    docker volume rm mongo-shard6a-data 2>/dev/null || true
    docker volume rm mongo-shard6b-data 2>/dev/null || true
    echo "✓ All MongoDB volumes removed"
else
    echo "Skipping volume removal - data preserved"
fi

echo ""
echo "=== Step 13: Removing MongoDB Network ==="
if docker network ls | grep -q mongo-cluster-network; then
    docker network rm mongo-cluster-network 2>/dev/null || true
    echo "✓ Network removed"
else
    echo "Network already removed or doesn't exist"
fi

echo ""
echo "=== Cleanup Summary ==="
echo "Checking remaining MongoDB containers..."
REMAINING=$(docker ps -a | grep -E "mongo|shard|config" | wc -l)

if [ $REMAINING -eq 0 ]; then
    echo "✓ All MongoDB containers removed successfully"
else
    echo "⚠ Warning: $REMAINING MongoDB-related containers still exist"
    docker ps -a | grep -E "mongo|shard|config"
fi

echo ""
echo "=========================================="
echo "MongoDB Sharded Cluster Cleanup Complete!"
echo "=========================================="
echo ""
echo "To verify cleanup:"
echo "  docker ps -a | grep mongo"
echo "  docker volume ls | grep mongo"
echo "  docker network ls | grep mongo"
echo ""
echo "To start fresh, run: "
echo "  docker-compose up -d"
echo "  & \"C:\Program Files\Git\bin\bash.exe\" .\mongo-up-6-shards.sh"