from kafka import KafkaConsumer
import json
from dotenv import load_dotenv
import os;
from pymongo import MongoClient, errors
import json
from datetime import datetime, timezone


load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")
MONGO_URI = os.getenv("MONGO_URI")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client.BDABD
    collection = db.taxi_events
    client.admin.command('ping')
    print("Connected to MongoDB shard successfully!")
except errors.ServerSelectionTimeoutError as e:
    print(f"Cannot connect to MongoDB: {e}")
    exit(1)

def borough_to_id(borough):
    file_path = 'borough_id.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
    else:
        data = {}
        data['next_id'] = 1
    
    if not data.get(borough,None):
        data[borough] = data['next_id']
        data['next_id'] += 1
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4) 
    
    return data[borough]

try:
    consumer = KafkaConsumer(
        'taxi_raw', 
        group_id='mongo', 
        bootstrap_servers=[KAFKA_BROKER_ADDRESS1 , KAFKA_BROKER_ADDRESS2], 
        auto_offset_reset='earliest', 
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening for messages...")
    try:
        for message in consumer:
            #print(f"Received message: Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Value: {message.value}")
            data = message.value

            pickup_location = data.get("PULocationID",{})
            pickup_datetime = data.get("tpep_pickup_datetime",None)
            pickup_borough = pickup_location.get("borough",None)
            
            dropoff_location = data.get("DOLocationID",{})
            dropoff_datetime = data.get("tpep_dropoff_datetime",None)
            dropoff_borough = dropoff_location.get("borough",None)

            trip_distance = data.get("trip_distance", None) #km
            duration = None         #h
            if pickup_datetime and dropoff_datetime:
                duration = (datetime.strptime(dropoff_datetime, "%Y-%m-%d %H:%M:%S") - datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S")).total_seconds() / 360

            speed = None            #km/h
            if trip_distance and duration:
                speed = trip_distance / duration

            price_per_km = None
            if trip_distance:
                price_per_km = data.get("fare_amount", 0) / trip_distance if trip_distance > 0 else 0

            mongo_doc = {
                "trip_id": data.get('trip_id',None),

                "pickup": {
                    "datetime": pickup_datetime,
                    "location": {
                        "location_id": pickup_location.get("LocationID",None),
                        "shape_leng": pickup_location.get('Shape_Leng',None),
                        "shape_area": pickup_location.get('Shape_Area',None),
                        "borough": pickup_borough,
                        "borough_id": borough_to_id(pickup_borough)
                    }
                },

                "dropoff": {
                    "datetime": dropoff_datetime,
                    "location": {
                        "location_id": dropoff_location.get("LocationID",None),
                        "shape_leng": dropoff_location.get('Shape_Leng',None),
                        "shape_area": dropoff_location.get('Shape_Area',None),
                        "borough": dropoff_borough,
                        "borough_id": borough_to_id(dropoff_borough)
                    }
                },

                "passenger_count": data.get("passenger_count",None),

                "payment": {
                    "payment_type": data.get("payment_type",None),
                    "fare_amount": data.get("fare_amount",None),
                    "extra": data.get("extra",None),
                    "mta_tax": data.get("mta_tax",None),
                    "tip_amount": data.get("tip_amount",None),
                    "tolls_amount": data.get("tolls_amount",None),
                    "improvement_surcharge": data.get("improvement_surcharge",None),
                    "congestion_surcharge": data.get("congestion_surcharge",None),
                    "airport_fee": data.get("Airport_fee",None),
                    "total_amount": data.get("total_amount",None)
                },

                "metrics": {
                "distance_km": trip_distance,
                "duration_h": round(duration, 2) if duration is not None else None,
                "speed_kmh": round(speed, 2) if speed is not None else None,
                "price_per_km": round(price_per_km, 2) if price_per_km is not None else None
            },


                "metadata": {
                    "vendor_id": data.get("VendorID",None),
                    "ratecode_id": data.get("RatecodeID",None),
                    "store_and_fwd_flag": data.get("store_and_fwd_flag",None),
                    "source": "kafka",
                    "insertion_time": f'{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}'
                }
            }
            
            try:
                collection.insert_one(data)
                consumer.commit()
                print(f"Inserted and commit the insert: {mongo_doc}")
            except errors.PyMongoError as e:
                print(f"Error inserting to MongoDB: {e}")

    finally:
        consumer.close()
        print(f"Consumer closed.")
        client.close()
        print(f"ClientMongo closed.")

except KeyboardInterrupt:
    print("\nExiting...")
