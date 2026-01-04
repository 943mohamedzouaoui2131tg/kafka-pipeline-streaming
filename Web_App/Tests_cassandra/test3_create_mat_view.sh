docker exec -i cassandra1 cqlsh <<'EOF'
USE projet_bd_rf3;

-- MV pour les revenus par borough
CREATE MATERIALIZED VIEW IF NOT EXISTS revenue_by_borough AS
    SELECT borough, year_month, pickup_date, pickup_hour, sum_total_amount
    FROM trips_by_borough_time
    WHERE borough IS NOT NULL 
      AND year_month IS NOT NULL 
      AND pickup_date IS NOT NULL
      AND pickup_hour IS NOT NULL
      AND sum_total_amount IS NOT NULL
    PRIMARY KEY (borough, sum_total_amount, year_month, pickup_date, pickup_hour)
    WITH CLUSTERING ORDER BY (sum_total_amount DESC);

-- MV pour l'analyse temporelle
CREATE MATERIALIZED VIEW IF NOT EXISTS trips_by_time AS
    SELECT year_month, pickup_date, pickup_hour, borough, sum_total_amount, sum_passenger_count
    FROM trips_by_borough_time
    WHERE borough IS NOT NULL 
      AND year_month IS NOT NULL 
      AND pickup_date IS NOT NULL
      AND pickup_hour IS NOT NULL
    PRIMARY KEY ((year_month, pickup_date), pickup_hour, borough)
    WITH CLUSTERING ORDER BY (pickup_hour ASC);
EOF