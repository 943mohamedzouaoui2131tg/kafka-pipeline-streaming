import os

import asyncio
import uuid
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
        source_file = "test.json"
        with open(f'../Data/datasets_json/{source_file}', 'r') as file:
            data_array = json.load(file)
            for item in data_array:
                data = item
                data["trip_id"] = uuid.uuid4().hex
                info = producer.send(topic_name, value=data, key=str(data['PULocationID']['borough']).encode())

                record_metadata = info.get(timeout=10)
                print(f"Record metadata: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
                await asyncio.sleep(0.5)
            
    except KafkaError as e :
        print(f"Error in producer: {e}")
    finally:
        producer.close()



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")