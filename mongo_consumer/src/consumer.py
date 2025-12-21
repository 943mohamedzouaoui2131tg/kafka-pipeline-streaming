from kafka import KafkaConsumer
import json
from dotenv import load_dotenv
import os;


load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")

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

    finally:
        consumer.close()
        print(f"Consumer closed.")

except KeyboardInterrupt:
        print("\nExiting...")
