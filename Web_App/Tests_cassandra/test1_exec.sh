# Exécuter le test d'ingestion
curl -X POST http://localhost:5000/cassandra/test/bulk-ingestion \
  -H "Content-Type: application/json" \
  -d '{
    "num_records": 100000,
    "batch_size": 100,
    "keyspace": "projet_bd_rf3"
  }'