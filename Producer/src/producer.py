import os

import asyncio

from kafka.errors import KafkaError
from dotenv import load_dotenv
from kafka import KafkaProducer
import json;


load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")

async def main():

    
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_ADDRESS1 , KAFKA_BROKER_ADDRESS2],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic_name = 'taxi_raw'
    try:
        i = 0
        while True:
            data = {'id': i, 'message': f'Hello Kafka from Python! {i}'}
            info = producer.send(topic_name, value=data , key=data['id'].to_bytes(4, byteorder='big'))
            print(f"Sent: {data}")
            record_metadata = info.get(timeout=10)
            print(f"Record metadata: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")

            await asyncio.sleep(1.5)
            i += 1
    except KafkaError as e :
        print(f"Error in producer: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")