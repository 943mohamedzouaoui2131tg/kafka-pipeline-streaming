#!/bin/bash

echo "=== Test de Partitionnement Rapide ==="

# Test avec seulement 1000 records
echo "Test 1: Borough + Hour"
curl -X POST http://localhost:5000/cassandra/test/partition-analysis \
  -H "Content-Type: application/json" \
  -d '{"strategy": "borough_hour", "num_records": 1000, "batch_size": 50}'

echo -e "\n\nTest 2: Time Bucket"
curl -X POST http://localhost:5000/cassandra/test/partition-analysis \
  -H "Content-Type: application/json" \
  -d '{"strategy": "time_bucket", "num_records": 1000, "batch_size": 50}'

echo -e "\n\nTest 3: UUID"
curl -X POST http://localhost:5000/cassandra/test/partition-analysis \
  -H "Content-Type: application/json" \
  -d '{"strategy": "uuid", "num_records": 1000, "batch_size": 50}'

echo -e "\n\nTest 4: Hash"
curl -X POST http://localhost:5000/cassandra/test/partition-analysis \
  -H "Content-Type: application/json" \
  -d '{"strategy": "hash", "num_records": 1000, "batch_size": 50}'

echo -e "\n\n=== Tests Terminés ==="