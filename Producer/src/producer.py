import os
import asyncio
import uuid
from kafka.errors import KafkaError
from dotenv import load_dotenv
from kafka import KafkaProducer
import json

load_dotenv()
KAFKA_BROKER_ADDRESS1 = os.getenv("KAFKA_BROKER1")
KAFKA_BROKER_ADDRESS2 = os.getenv("KAFKA_BROKER2")

DATASET_DIR = "../Data/datasets_json"
TOPIC_NAME = "taxi_raw"

def validate_record(data):
    """Validate that record has all required fields"""
    try:
        # Check required nested fields
        if not isinstance(data.get("PULocationID"), dict):
            return False, "Missing or invalid PULocationID"
        
        if not isinstance(data.get("DOLocationID"), dict):
            return False, "Missing or invalid DOLocationID"
        
        # Check required fields in PULocationID
        pu_loc = data["PULocationID"]
        required_pu_fields = ["borough", "zone", "LocationID"]
        for field in required_pu_fields:
            if field not in pu_loc:
                return False, f"Missing PULocationID.{field}"
        
        # Check required fields in DOLocationID
        do_loc = data["DOLocationID"]
        required_do_fields = ["zone", "LocationID"]
        for field in required_do_fields:
            if field not in do_loc:
                return False, f"Missing DOLocationID.{field}"
        
        # Check required top-level fields
        required_fields = [
            "tpep_pickup_datetime", 
            "tpep_dropoff_datetime",
            "VendorID"
        ]
        for field in required_fields:
            if field not in data:
                return False, f"Missing field: {field}"
        
        return True, None
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

async def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_ADDRESS1, KAFKA_BROKER_ADDRESS2],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    stats = {
        "total_records": 0,
        "sent_records": 0,
        "skipped_records": 0,
        "files_processed": 0
    }

    try:
        json_files = sorted(
            f for f in os.listdir(DATASET_DIR) if f.endswith(".json")
        )

        print(f"Found {len(json_files)} JSON files to process")
        print(f"{'='*70}\n")

        for source_file in json_files:
            print(f"📂 Processing file: {source_file}")
            file_records = 0
            file_skipped = 0

            try:
                with open(os.path.join(DATASET_DIR, source_file), "r") as file:
                    data_array = json.load(file)

                    for idx, item in enumerate(data_array):
                        stats["total_records"] += 1
                        data = item
                        
                        # Validate record
                        is_valid, error_msg = validate_record(data)
                        
                        if not is_valid:
                            stats["skipped_records"] += 1
                            file_skipped += 1
                            print(f"  ⚠️  Skipped record {idx + 1}: {error_msg}")
                            continue
                        
                        # Add unique trip ID
                        data["trip_id"] = uuid.uuid4().hex

                        # Get borough for partitioning key
                        borough = str(data["PULocationID"]["borough"])

                        # Send to Kafka
                        info = producer.send(
                            TOPIC_NAME,
                            value=data,
                            key=borough.encode()
                        )

                        record_metadata = info.get(timeout=10)
                        stats["sent_records"] += 1
                        file_records += 1
                        
                        # Print progress every 100 records
                        if file_records % 100 == 0:
                            print(f"  ✅ Sent {file_records} records from {source_file}")

                stats["files_processed"] += 1
                print(f"  ✓ File complete: {file_records} sent, {file_skipped} skipped")
                print(f"  Last record: topic={record_metadata.topic}, "
                      f"partition={record_metadata.partition}, "
                      f"offset={record_metadata.offset}\n")

            except json.JSONDecodeError as e:
                print(f"  ❌ JSON decode error in {source_file}: {e}\n")
                continue
            except Exception as e:
                print(f"  ❌ Error processing {source_file}: {e}\n")
                continue

        # Print final statistics
        print(f"\n{'='*70}")
        print(f"📊 FINAL STATISTICS")
        print(f"{'='*70}")
        print(f"  Files processed    : {stats['files_processed']}/{len(json_files)}")
        print(f"  Total records      : {stats['total_records']}")
        print(f"  Records sent       : {stats['sent_records']}")
        print(f"  Records skipped    : {stats['skipped_records']}")
        if stats['total_records'] > 0:
            success_rate = (stats['sent_records'] / stats['total_records']) * 100
            print(f"  Success rate       : {success_rate:.2f}%")
        print(f"{'='*70}\n")

    except KafkaError as e:
        print(f"❌ Kafka error: {e}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print("Closing producer...")
        producer.close()
        print("✅ Producer closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user (Ctrl+C)")
        print("✅ Exiting...")