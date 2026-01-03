import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import os
import json
from pathlib import Path

def load_taxi_zones(shapefile_path):
    """Load taxi zone data from shapefile."""
    try:
        zones = gpd.read_file(shapefile_path)
        zone_dict = {}
        for _, row in zones.iterrows():
            location_id = row.get('LocationID') or row.get('location_id') or row.get('OBJECTID')
            if location_id:
                zone_data = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                if 'geometry' in row.index:
                    zone_data['geometry_type'] = str(row['geometry'].geom_type)
                zone_dict[int(location_id)] = zone_data
        return zone_dict
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        return {}

def enrich_record(record, zone_dict):
    """Enrich a single record with zone information."""
    if 'PULocationID' in record and pd.notna(record['PULocationID']):
        pu_id = int(record['PULocationID'])
        record['PULocationID'] = zone_dict.get(pu_id, {'LocationID': pu_id})

    if 'DOLocationID' in record and pd.notna(record['DOLocationID']):
        do_id = int(record['DOLocationID'])
        record['DOLocationID'] = zone_dict.get(do_id, {'LocationID': do_id})

    return record

def convert_parquet_to_json_chunked(input_folder, output_folder, shapefile_path, batch_size=100000):
    """
    Convert all Parquet files in input folder to multiple JSON files,
    each containing exactly 100,000 records (except last file).
    """
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    print("Loading taxi zone data...")
    zone_dict = load_taxi_zones(shapefile_path)
    print(f"Loaded {len(zone_dict)} taxi zones")

    parquet_files = list(Path(input_folder).glob('*.parquet'))

    if not parquet_files:
        print(f"No .parquet files found in {input_folder}")
        return

    MAX_RECORDS_PER_FILE = 100_000

    for parquet_file in parquet_files:
        try:
            print(f"\nProcessing: {parquet_file.name}")

            parquet_file_obj = pq.ParquetFile(parquet_file)
            total_rows = parquet_file_obj.metadata.num_rows
            print(f"  Total records: {total_rows:,}")

            part_number = 1
            records_in_current_file = 0
            total_processed = 0
            first_record = True
            f = None

            def open_new_file():
                nonlocal f, first_record, records_in_current_file, part_number
                if f:
                    f.write('\n]')
                    f.close()

                json_filename = f"{parquet_file.stem}_part{part_number:03d}.json"
                json_path = Path(output_folder) / json_filename
                f = open(json_path, 'w', encoding='utf-8')
                f.write('[')
                first_record = True
                records_in_current_file = 0
                print(f"  → Writing {json_filename}")
                part_number += 1

            open_new_file()

            for batch in parquet_file_obj.iter_batches(batch_size=batch_size):
                batch_df = batch.to_pandas()
                records = batch_df.to_dict('records')

                for record in records:
                    if records_in_current_file >= MAX_RECORDS_PER_FILE:
                        open_new_file()

                    enriched_record = enrich_record(record, zone_dict)

                    if not first_record:
                        f.write(',\n')
                    else:
                        f.write('\n')
                        first_record = False

                    json.dump(enriched_record, f, indent=2, default=str)

                    records_in_current_file += 1
                    total_processed += 1

                del batch_df, records
                print(f"  Processed: {total_processed:,} / {total_rows:,}", end='\r')

            if f:
                f.write('\n]')
                f.close()

            print(f"\n  ✓ Finished {parquet_file.name}")

        except Exception as e:
            print(f"\n  ✗ Error processing {parquet_file.name}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    INPUT_FOLDER = "datasets_parquet"
    OUTPUT_FOLDER = "datasets_json"
    SHAPEFILE_PATH = "taxi_zones/taxi_zones.shp"
    BATCH_SIZE = 50000

    if not os.path.exists(SHAPEFILE_PATH):
        print(f"Error: Shapefile not found at {SHAPEFILE_PATH}")
        exit(1)

    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Input folder not found at {INPUT_FOLDER}")
        exit(1)

    convert_parquet_to_json_chunked(INPUT_FOLDER, OUTPUT_FOLDER, SHAPEFILE_PATH, BATCH_SIZE)
    print("\n✓ Conversion complete!")