import eventlet
eventlet.monkey_patch()


from kafka import KafkaConsumer
import json
from dotenv import load_dotenv
import os;
from cassandra.cluster import Cluster 



load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")
CASSANDRA_BROKER = os.getenv("CASSANDRA_BROKER")




try:
    consumer = KafkaConsumer(
            'taxi_raw', 
            group_id='cassandra', 
            bootstrap_servers=[KAFKA_BROKER_ADDRESS1 , KAFKA_BROKER_ADDRESS2], 
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
        )

    print("Listening for messages...")

    print("Connected to Cassandra")
    cluster = Cluster(["localhost"], port=3000)  # Connect to Cassandra on port 3000 in the local machine not docker containers
    session = cluster.connect()
    print("Connected to Cassandra cluster")

    session.execute("USE Projet_bd;")

    try:
        for message in consumer:
            print(f"Received message: Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Value: {message.value}")
    finally:
        consumer.close()
    print(f"Consumer closed.")
    
except KeyboardInterrupt:
    print("\nExiting...")   