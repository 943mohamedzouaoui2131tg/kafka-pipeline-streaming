#!/bin/sh

docker exec -i cassandra1 cqlsh <<'EOF'
CREATE KEYSPACE IF NOT EXISTS Projet_bd_Rf2
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 2
};

USE Projet_bd_Rf2;

CREATE TABLE IF NOT EXISTS trips_by_borough_time (
    borough TEXT,
    year_month TEXT,
    pickup_date DATE,
    pickup_hour INT,

    sum_passenger_count DOUBLE,
    count_rate_standard BIGINT,
    count_rate_jfk BIGINT,
    count_rate_newark BIGINT,
    count_rate_nassau BIGINT,
    count_rate_negotiated BIGINT,
    count_rate_group BIGINT,

    count_payment_card BIGINT,
    count_payment_cash BIGINT,
    count_payment_free BIGINT,
    count_payment_dispute BIGINT,
    count_payment_unknown BIGINT,
    count_payment_voided BIGINT,

    sum_fare_amount DOUBLE,
    sum_mta_tax DOUBLE,
    sum_tip_amount DOUBLE,
    sum_total_amount DOUBLE,

    count_vendor_cmt BIGINT,
    count_vendor_verifone BIGINT,

    PRIMARY KEY ((borough, year_month), pickup_date, pickup_hour)
) WITH CLUSTERING ORDER BY (pickup_date ASC, pickup_hour ASC);

CREATE TABLE IF NOT EXISTS trips_by_pickup_zone (
    borough TEXT,
    year_month TEXT,
    pickup_date DATE,
    pickup_hour INT,
    pickup_zone TEXT,


    sum_passenger_count DOUBLE,
    avg_trip_distance DOUBLE,
    sum_fare_amount DOUBLE,
    sum_tip_amount DOUBLE,
    sum_total_amount DOUBLE,

    count_payment_card BIGINT,
    count_payment_cash BIGINT,
    count_payment_free BIGINT,
    count_payment_dispute BIGINT,
    count_payment_unknown BIGINT,
    count_payment_voided BIGINT,

    count_vendor_cmt BIGINT,
    count_vendor_verifone BIGINT,

    PRIMARY KEY ((borough, year_month), pickup_date, pickup_hour, pickup_zone)
) WITH CLUSTERING ORDER BY (pickup_date ASC, pickup_hour ASC, pickup_zone ASC);

CREATE TABLE IF NOT EXISTS trips_by_route (
    pickup_location_id INT,
    pickup_zone TEXT,
    dropoff_location_id INT,
    dropoff_zone TEXT,
    year_month TEXT,
    pickup_date DATE,

    avg_trip_distance DOUBLE,
    avg_duration INT,
    avg_fare_amount DOUBLE,
    avg_tip_amount DOUBLE,
    avg_passenger_count DOUBLE,

    count_payment_card BIGINT,
    count_payment_cash BIGINT,
    count_payment_free BIGINT,
    count_payment_dispute BIGINT,
    count_payment_unknown BIGINT,
    count_payment_voided BIGINT,

    PRIMARY KEY ((pickup_location_id, year_month),
                 dropoff_location_id, pickup_date)
) WITH CLUSTERING ORDER BY (dropoff_location_id ASC, pickup_date DESC);

CREATE TABLE IF NOT EXISTS trips_by_vendor (
    vendor_id INT,
    borough TEXT,
    year_month TEXT,
    pickup_date DATE,
    pickup_location_id INT,

    sum_passenger_count DOUBLE,
    avg_trip_distance DOUBLE,
    sum_total_amount DOUBLE,
    sum_tip_amount DOUBLE,
    sum_mta_tax DOUBLE,

    count_payment_card BIGINT,
    count_payment_cash BIGINT,
    count_payment_free BIGINT,
    count_payment_dispute BIGINT,
    count_payment_unknown BIGINT,
    count_payment_voided BIGINT,

    PRIMARY KEY ((vendor_id, borough, year_month),
                 pickup_date, pickup_location_id)
) WITH CLUSTERING ORDER BY (pickup_date DESC, pickup_location_id ASC);
EOF
