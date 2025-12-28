import eventlet
eventlet.monkey_patch()

from kafka import KafkaConsumer
import json
from dotenv import load_dotenv
import os
from cassandra.cluster import Cluster
from datetime import datetime
from collections import defaultdict

load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")
CASSANDRA_BROKER = os.getenv("CASSANDRA_BROKER")

# Buffers pour chaque table
buffer_borough_time = defaultdict(lambda: {
    'sum_passenger_count': 0.0, 'sum_fare_amount': 0.0, 'sum_mta_tax': 0.0,
    'sum_tip_amount': 0.0, 'sum_total_amount': 0.0,
    'count_rate_standard': 0, 'count_rate_jfk': 0, 'count_rate_newark': 0,
    'count_rate_nassau': 0, 'count_rate_negotiated': 0, 'count_rate_group': 0,
    'count_payment_card': 0, 'count_payment_cash': 0, 'count_payment_free': 0,
    'count_payment_dispute': 0, 'count_payment_unknown': 0, 'count_payment_voided': 0,
    'count_vendor_cmt': 0, 'count_vendor_verifone': 0,
})

buffer_pickup_zone = defaultdict(lambda: {
    'sum_passenger_count': 0.0, 'sum_trip_distance': 0.0, 'trip_count': 0,
    'sum_fare_amount': 0.0, 'sum_tip_amount': 0.0, 'sum_total_amount': 0.0,
    'count_payment_card': 0, 'count_payment_cash': 0, 'count_payment_free': 0,
    'count_payment_dispute': 0, 'count_payment_unknown': 0, 'count_payment_voided': 0,
    'count_vendor_cmt': 0, 'count_vendor_verifone': 0,
})

buffer_route = defaultdict(lambda: {
    'sum_trip_distance': 0.0, 'sum_duration': 0.0, 'sum_fare_amount': 0.0,
    'sum_tip_amount': 0.0, 'sum_passenger_count': 0.0, 'trip_count': 0,
    'count_payment_card': 0, 'count_payment_cash': 0, 'count_payment_free': 0,
    'count_payment_dispute': 0, 'count_payment_unknown': 0, 'count_payment_voided': 0,
})

buffer_vendor = defaultdict(lambda: {
    'sum_passenger_count': 0.0, 'sum_trip_distance': 0.0, 'sum_total_amount': 0.0,
    'sum_tip_amount': 0.0, 'sum_mta_tax': 0.0, 'trip_count': 0,
    'count_payment_card': 0, 'count_payment_cash': 0, 'count_payment_free': 0,
    'count_payment_dispute': 0, 'count_payment_unknown': 0, 'count_payment_voided': 0,
})

BATCH_SIZE = 100

def aggregate_trip_in_buffers(trip):
    """Agrège dans tous les buffers en une seule passe"""
    try:
        pickup_dt = datetime.strptime(trip['tpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S')
        dropoff_dt = datetime.strptime(trip['tpep_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
        
        borough = trip['PULocationID']['borough']
        year_month = pickup_dt.strftime('%Y-%m')
        pickup_date = pickup_dt.date()
        pickup_hour = pickup_dt.hour
        pickup_zone = trip['PULocationID']['zone']
        pickup_location_id = trip['PULocationID']['LocationID']
        dropoff_location_id = trip['DOLocationID']['LocationID']
        dropoff_zone = trip['DOLocationID']['zone']
        vendor_id = trip.get('VendorID')
        
        # Service zone (à déterminer selon vos règles métier)
        service_zone = "Manhattan" if borough == "Manhattan" else "Other"
        
        # Calcul de la durée en secondes
        duration = (dropoff_dt - pickup_dt).total_seconds()
        
        # Valeurs communes
        passenger_count = float(trip.get('passenger_count', 0))
        trip_distance = float(trip.get('trip_distance', 0))
        fare_amount = float(trip.get('fare_amount', 0))
        tip_amount = float(trip.get('tip_amount', 0))
        total_amount = float(trip.get('total_amount', 0))
        mta_tax = float(trip.get('mta_tax', 0))
        payment_type = trip.get('payment_type')
        rate_code = trip.get('RatecodeID')
        
        # 1. Buffer trips_by_borough_time
        key1 = (borough, year_month, pickup_date, pickup_hour)
        buf1 = buffer_borough_time[key1]
        buf1['sum_passenger_count'] += passenger_count
        buf1['sum_fare_amount'] += fare_amount
        buf1['sum_mta_tax'] += mta_tax
        buf1['sum_tip_amount'] += tip_amount
        buf1['sum_total_amount'] += total_amount
        buf1['count_rate_standard'] += 1 if rate_code == 1 else 0
        buf1['count_rate_jfk'] += 1 if rate_code == 2 else 0
        buf1['count_rate_newark'] += 1 if rate_code == 3 else 0
        buf1['count_rate_nassau'] += 1 if rate_code == 4 else 0
        buf1['count_rate_negotiated'] += 1 if rate_code == 5 else 0
        buf1['count_rate_group'] += 1 if rate_code == 6 else 0
        buf1['count_payment_card'] += 1 if payment_type == 1 else 0
        buf1['count_payment_cash'] += 1 if payment_type == 2 else 0
        buf1['count_payment_free'] += 1 if payment_type == 3 else 0
        buf1['count_payment_dispute'] += 1 if payment_type == 4 else 0
        buf1['count_payment_unknown'] += 1 if payment_type == 5 else 0
        buf1['count_payment_voided'] += 1 if payment_type == 6 else 0
        buf1['count_vendor_cmt'] += 1 if vendor_id == 1 else 0
        buf1['count_vendor_verifone'] += 1 if vendor_id == 2 else 0
        
        # 2. Buffer trips_by_pickup_zone
        key2 = (borough, year_month, pickup_date, pickup_hour, pickup_zone, service_zone)
        buf2 = buffer_pickup_zone[key2]
        buf2['sum_passenger_count'] += passenger_count
        buf2['sum_trip_distance'] += trip_distance
        buf2['trip_count'] += 1
        buf2['sum_fare_amount'] += fare_amount
        buf2['sum_tip_amount'] += tip_amount
        buf2['sum_total_amount'] += total_amount
        buf2['count_payment_card'] += 1 if payment_type == 1 else 0
        buf2['count_payment_cash'] += 1 if payment_type == 2 else 0
        buf2['count_payment_free'] += 1 if payment_type == 3 else 0
        buf2['count_payment_dispute'] += 1 if payment_type == 4 else 0
        buf2['count_payment_unknown'] += 1 if payment_type == 5 else 0
        buf2['count_payment_voided'] += 1 if payment_type == 6 else 0
        buf2['count_vendor_cmt'] += 1 if vendor_id == 1 else 0
        buf2['count_vendor_verifone'] += 1 if vendor_id == 2 else 0
        
        # 3. Buffer trips_by_route
        key3 = (pickup_location_id, year_month, dropoff_location_id, pickup_date, pickup_zone, dropoff_zone)
        buf3 = buffer_route[key3]
        buf3['sum_trip_distance'] += trip_distance
        buf3['sum_duration'] += duration
        buf3['sum_fare_amount'] += fare_amount
        buf3['sum_tip_amount'] += tip_amount
        buf3['sum_passenger_count'] += passenger_count
        buf3['trip_count'] += 1
        buf3['count_payment_card'] += 1 if payment_type == 1 else 0
        buf3['count_payment_cash'] += 1 if payment_type == 2 else 0
        buf3['count_payment_free'] += 1 if payment_type == 3 else 0
        buf3['count_payment_dispute'] += 1 if payment_type == 4 else 0
        buf3['count_payment_unknown'] += 1 if payment_type == 5 else 0
        buf3['count_payment_voided'] += 1 if payment_type == 6 else 0
        
        # 4. Buffer trips_by_vendor
        key4 = (vendor_id, borough, year_month, pickup_date, pickup_location_id)
        buf4 = buffer_vendor[key4]
        buf4['sum_passenger_count'] += passenger_count
        buf4['sum_trip_distance'] += trip_distance
        buf4['sum_total_amount'] += total_amount
        buf4['sum_tip_amount'] += tip_amount
        buf4['sum_mta_tax'] += mta_tax
        buf4['trip_count'] += 1
        buf4['count_payment_card'] += 1 if payment_type == 1 else 0
        buf4['count_payment_cash'] += 1 if payment_type == 2 else 0
        buf4['count_payment_free'] += 1 if payment_type == 3 else 0
        buf4['count_payment_dispute'] += 1 if payment_type == 4 else 0
        buf4['count_payment_unknown'] += 1 if payment_type == 5 else 0
        buf4['count_payment_voided'] += 1 if payment_type == 6 else 0
        
        return True
    except Exception as e:
        print(f"❌ Erreur agrégation: {e}")
        return False

def flush_all_buffers_to_cassandra(session):
    """Flush tous les buffers vers Cassandra"""
    total_written = 0
    
    # 1. Flush trips_by_borough_time
    if buffer_borough_time:
        print(f"\n🔄 Flush table 1/4: trips_by_borough_time ({len(buffer_borough_time)} lignes)")
        query = session.prepare("""
            INSERT INTO trips_by_borough_time (
                borough, year_month, pickup_date, pickup_hour,
                sum_passenger_count, sum_fare_amount, sum_mta_tax, 
                sum_tip_amount, sum_total_amount,
                count_rate_standard, count_rate_jfk, count_rate_newark,
                count_rate_nassau, count_rate_negotiated, count_rate_group,
                count_payment_card, count_payment_cash, count_payment_free,
                count_payment_dispute, count_payment_unknown, count_payment_voided,
                count_vendor_cmt, count_vendor_verifone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for key, data in buffer_borough_time.items():
            borough, year_month, pickup_date, pickup_hour = key
            session.execute(query, (
                borough, year_month, pickup_date, pickup_hour,
                data['sum_passenger_count'], data['sum_fare_amount'], data['sum_mta_tax'],
                data['sum_tip_amount'], data['sum_total_amount'],
                data['count_rate_standard'], data['count_rate_jfk'], data['count_rate_newark'],
                data['count_rate_nassau'], data['count_rate_negotiated'], data['count_rate_group'],
                data['count_payment_card'], data['count_payment_cash'], data['count_payment_free'],
                data['count_payment_dispute'], data['count_payment_unknown'], data['count_payment_voided'],
                data['count_vendor_cmt'], data['count_vendor_verifone']
            ))
            total_written += 1
        buffer_borough_time.clear()
    
    # 2. Flush trips_by_pickup_zone
    if buffer_pickup_zone:
        print(f"🔄 Flush table 2/4: trips_by_pickup_zone ({len(buffer_pickup_zone)} lignes)")
        query = session.prepare("""
            INSERT INTO trips_by_pickup_zone (
                borough, year_month, pickup_date, pickup_hour, pickup_zone, service_zone,
                sum_passenger_count, avg_trip_distance, sum_fare_amount,
                sum_tip_amount, sum_total_amount,
                count_payment_card, count_payment_cash, count_payment_free,
                count_payment_dispute, count_payment_unknown, count_payment_voided,
                count_vendor_cmt, count_vendor_verifone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for key, data in buffer_pickup_zone.items():
            borough, year_month, pickup_date, pickup_hour, pickup_zone, service_zone = key
            avg_distance = data['sum_trip_distance'] / data['trip_count'] if data['trip_count'] > 0 else 0.0
            session.execute(query, (
                borough, year_month, pickup_date, pickup_hour, pickup_zone, service_zone,
                data['sum_passenger_count'], avg_distance, data['sum_fare_amount'],
                data['sum_tip_amount'], data['sum_total_amount'],
                data['count_payment_card'], data['count_payment_cash'], data['count_payment_free'],
                data['count_payment_dispute'], data['count_payment_unknown'], data['count_payment_voided'],
                data['count_vendor_cmt'], data['count_vendor_verifone']
            ))
            total_written += 1
        buffer_pickup_zone.clear()
    
    # 3. Flush trips_by_route
    if buffer_route:
        print(f"🔄 Flush table 3/4: trips_by_route ({len(buffer_route)} lignes)")
        query = session.prepare("""
            INSERT INTO trips_by_route (
                pickup_location_id, pickup_zone, dropoff_location_id, dropoff_zone,
                year_month, pickup_date,
                avg_trip_distance, avg_duration, avg_fare_amount,
                avg_tip_amount, avg_passenger_count,
                count_payment_card, count_payment_cash, count_payment_free,
                count_payment_dispute, count_payment_unknown, count_payment_voided
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for key, data in buffer_route.items():
            pickup_loc_id, year_month, dropoff_loc_id, pickup_date, pickup_zone, dropoff_zone = key
            trip_count = data['trip_count']
            avg_distance = data['sum_trip_distance'] / trip_count if trip_count > 0 else 0.0
            avg_duration = int(data['sum_duration'] / trip_count) if trip_count > 0 else 0
            avg_fare = data['sum_fare_amount'] / trip_count if trip_count > 0 else 0.0
            avg_tip = data['sum_tip_amount'] / trip_count if trip_count > 0 else 0.0
            avg_passenger = data['sum_passenger_count'] / trip_count if trip_count > 0 else 0.0
            
            session.execute(query, (
                pickup_loc_id, pickup_zone, dropoff_loc_id, dropoff_zone,
                year_month, pickup_date,
                avg_distance, avg_duration, avg_fare, avg_tip, avg_passenger,
                data['count_payment_card'], data['count_payment_cash'], data['count_payment_free'],
                data['count_payment_dispute'], data['count_payment_unknown'], data['count_payment_voided']
            ))
            total_written += 1
        buffer_route.clear()
    
    # 4. Flush trips_by_vendor
    if buffer_vendor:
        print(f"🔄 Flush table 4/4: trips_by_vendor ({len(buffer_vendor)} lignes)")
        query = session.prepare("""
            INSERT INTO trips_by_vendor (
                vendor_id, borough, year_month, pickup_date, pickup_location_id,
                sum_passenger_count, avg_trip_distance, sum_total_amount,
                sum_tip_amount, sum_mta_tax,
                count_payment_card, count_payment_cash, count_payment_free,
                count_payment_dispute, count_payment_unknown, count_payment_voided
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for key, data in buffer_vendor.items():
            vendor_id, borough, year_month, pickup_date, pickup_loc_id = key
            avg_distance = data['sum_trip_distance'] / data['trip_count'] if data['trip_count'] > 0 else 0.0
            
            session.execute(query, (
                vendor_id, borough, year_month, pickup_date, pickup_loc_id,
                data['sum_passenger_count'], avg_distance, data['sum_total_amount'],
                data['sum_tip_amount'], data['sum_mta_tax'],
                data['count_payment_card'], data['count_payment_cash'], data['count_payment_free'],
                data['count_payment_dispute'], data['count_payment_unknown'], data['count_payment_voided']
            ))
            total_written += 1
        buffer_vendor.clear()
    
    print(f"✅ Total: {total_written} lignes écrites dans 4 tables\n")
    return total_written

try:
    consumer = KafkaConsumer(
        'taxi_raw', 
        group_id='cassandra', 
        bootstrap_servers=[KAFKA_BROKER_ADDRESS1, KAFKA_BROKER_ADDRESS2], 
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
    )

    print("🎧 Listening for messages...")

    cluster = Cluster(["localhost"], port=3000)
    session = cluster.connect()
    print("✅ Connected to Cassandra")

    session.execute("USE Projet_bd-Rf3;")
    print("✅ Using keyspace: Projet_bd-Rf3")
    print(f"⏱️  Arrêt automatique après inactivité\n")

    try:
        message_count = 0
        timeout_seconds = 5
        no_message_count = 0
        max_empty_polls = 5
        
        while True:
            messages = consumer.poll(timeout_ms=timeout_seconds * 1000, max_records=500)
            
            if not messages:
                no_message_count += 1
                print(f"⏳ Aucun message ({no_message_count}/{max_empty_polls})...")
                
                if no_message_count >= max_empty_polls:
                    print(f"\n✅ Tous les messages traités")
                    break
                continue
            
            no_message_count = 0
            
            for topic_partition, records in messages.items():
                for message in records:
                    message_count += 1
                    trip_data = message.value
                    
                    if aggregate_trip_in_buffers(trip_data):
                        if message_count % 50 == 0:
                            print(f"📊 {message_count} messages | Buffers: T1={len(buffer_borough_time)}, T2={len(buffer_pickup_zone)}, T3={len(buffer_route)}, T4={len(buffer_vendor)}")
                    
                    if message_count % BATCH_SIZE == 0:
                        flush_all_buffers_to_cassandra(session)
                
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption, flush final...")
        flush_all_buffers_to_cassandra(session)
    finally:
        flush_all_buffers_to_cassandra(session)
        consumer.close()
        cluster.shutdown()
        print(f"\n✅ Total: {message_count} messages traités")
    
except Exception as e:
    print(f"\n❌ Erreur: {e}")
    import traceback
    traceback.print_exc()