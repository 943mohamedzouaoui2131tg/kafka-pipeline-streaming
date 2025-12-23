from kafka import KafkaConsumer
import json
from dotenv import load_dotenv
import os;
from pymongo import MongoClient, errors


load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")
MONGO_URI = os.getenv("MONGO_URI")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client.BDABD
    collection = db.taxi_events
    # Test de connexion
    client.admin.command('ping')
    print("Connected to MongoDB shard successfully!")
except errors.ServerSelectionTimeoutError as e:
    print(f"Cannot connect to MongoDB: {e}")
    exit(1)

try:
    consumer = KafkaConsumer(
        'taxi_raw', 
        group_id='mongo', 
        bootstrap_servers=[KAFKA_BROKER_ADDRESS1 , KAFKA_BROKER_ADDRESS2], 
        auto_offset_reset='earliest', 
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening for messages...")
    try:
        for message in consumer:
            print(f"Received message: Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Value: {message.value}")
            data = message.value
            if 'pickup_borough_id' not in data:
                data['pickup_borough_id'] = (data.get('id') % 3) + 1
            try:
                collection.insert_one(data)
            except errors.PyMongoError as e:
                print(f"Error inserting to MongoDB: {e}")

    finally:
        consumer.close()
        print(f"Consumer closed.")

except KeyboardInterrupt:
        print("\nExiting...")
