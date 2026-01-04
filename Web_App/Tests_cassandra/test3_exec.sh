#!/bin/bash

echo "=== Test de Requêtes Analytiques ==="

# Test sur Manhattan avec 2024-01
echo "Test 1: Manhattan - Janvier 2024"
curl "http://localhost:5000/cassandra/test/query-benchmark?borough=Manhattan&year_month=2024-01"

echo -e "\n\n=================================\n"

echo "Test 2: Brooklyn - Janvier 2024"
curl "http://localhost:5000/cassandra/test/query-benchmark?borough=Brooklyn&year_month=2024-01"

echo -e "\n\n=================================\n"

echo "Test 3: Queens - Janvier 2024"
curl "http://localhost:5000/cassandra/test/query-benchmark?borough=Queens&year_month=2024-01"

echo -e "\n\n=== Tests Terminés ==="

