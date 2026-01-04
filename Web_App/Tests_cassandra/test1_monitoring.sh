# monitoring.sh
#!/bin/bash

echo "=== Monitoring Cassandra Cluster ==="
while true; do
    clear
    echo "Timestamp: $(date)"
    echo "================================"
    
    # Nombre de lignes dans chaque table
    docker exec cassandra1 cqlsh -e "SELECT COUNT(*) FROM projet_bd_rf3.trips_by_borough_time;" 2>/dev/null | grep -E '[0-9]+'
    
    # CPU et Mémoire
    docker stats --no-stream cassandra1 cassandra2 cassandra3 | grep cassandra
    
    # Latence réseau
    docker exec cassandra1 nodetool status
    
    sleep 5
done