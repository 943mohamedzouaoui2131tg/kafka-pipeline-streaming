
docker exec -i cassandra1 cqlsh <<'EOF'
USE projet_bd_rf3;

-- Stratégie 1: Borough + Hour (actuel)
CREATE TABLE IF NOT EXISTS test_partition_borough_hour (
    borough TEXT,
    hour INT,
    year_month TEXT,
    pickup_date DATE,
    sum_total_amount DOUBLE,
    PRIMARY KEY ((borough, hour), year_month, pickup_date)
);

-- Stratégie 2: Temporal buckets (fenêtres de 6 heures)
CREATE TABLE IF NOT EXISTS test_partition_time_bucket (
    time_bucket TEXT,  -- format: "2013-01-01-00" (date + 6h bucket)
    borough TEXT,
    pickup_hour INT,
    pickup_date DATE,
    sum_total_amount DOUBLE,
    PRIMARY KEY (time_bucket, borough, pickup_date, pickup_hour)
);

-- Stratégie 3: UUID aléatoire (équilibrage parfait)
CREATE TABLE IF NOT EXISTS test_partition_uuid (
    partition_id UUID,
    borough TEXT,
    year_month TEXT,
    pickup_date DATE,
    pickup_hour INT,
    sum_total_amount DOUBLE,
    PRIMARY KEY (partition_id, pickup_date, pickup_hour)
);

-- Stratégie 4: Hash composite (borough + date)
CREATE TABLE IF NOT EXISTS test_partition_hash (
    partition_hash TEXT,  -- MD5(borough + date) % 100
    borough TEXT,
    year_month TEXT,
    pickup_date DATE,
    pickup_hour INT,
    sum_total_amount DOUBLE,
    PRIMARY KEY (partition_hash, borough, pickup_date, pickup_hour)
);
EOF