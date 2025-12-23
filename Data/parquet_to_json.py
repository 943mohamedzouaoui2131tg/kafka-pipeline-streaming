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
        # Convert to regular dict with LocationID as key
        zone_dict = {}
        for _, row in zones.iterrows():
            location_id = row.get('LocationID') or row.get('location_id') or row.get('OBJECTID')
            if location_id:
                # Convert all zone data to dict, excluding geometry for now
                zone_data = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                # Add simplified geometry if needed (convert to string to reduce size)
                if 'geometry' in row.index:
                    # Store only simplified geometry info
                    zone_data['geometry_type'] = str(row['geometry'].geom_type)
                zone_dict[int(location_id)] = zone_data
        return zone_dict
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        return {}

def enrich_record(record, zone_dict):
    """Enrich a single record with zone information."""
    # Enrich pickup location
    if 'PULocationID' in record and pd.notna(record['PULocationID']):
        pu_id = int(record['PULocationID'])
        record['PULocationID'] = zone_dict.get(pu_id, {'LocationID': pu_id})
    
    # Enrich dropoff location
    if 'DOLocationID' in record and pd.notna(record['DOLocationID']):
        do_id = int(record['DOLocationID'])
        record['DOLocationID'] = zone_dict.get(do_id, {'LocationID': do_id})
    
    return record

def convert_parquet_to_json_chunked(input_folder, output_folder, shapefile_path, batch_size=100000):
    """
    Convert all Parquet files in input folder to JSON in output folder,
    enriching with taxi zone geographic data.
    Processes files in batches to handle large datasets.
    """
    # Create output folder if it doesn't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    # Load taxi zones
    print("Loading taxi zone data...")
    zone_dict = load_taxi_zones(shapefile_path)
    print(f"Loaded {len(zone_dict)} taxi zones")
    
    # Get all parquet files
    parquet_files = list(Path(input_folder).glob('*.parquet'))
    
    if not parquet_files:
        print(f"No .parquet files found in {input_folder}")
        return
    
    print(f"\nFound {len(parquet_files)} parquet file(s) to convert")
    print(f"Processing in batches of {batch_size:,} records\n")
    
    # Process each file
    for parquet_file in parquet_files:
        try:
            print(f"Processing: {parquet_file.name}")
            
            # Open parquet file with PyArrow
            parquet_file_obj = pq.ParquetFile(parquet_file)
            total_rows = parquet_file_obj.metadata.num_rows
            print(f"  Total records: {total_rows:,}")
            
            # Generate output filename
            json_filename = parquet_file.stem + '.json'
            json_path = Path(output_folder) / json_filename
            
            # Process in batches and write to file
            first_record = True
            batches_processed = 0
            records_processed = 0
            
            with open(json_path, 'w', encoding='utf-8') as f:
                f.write('[')  # Start JSON array
                
                # Read and process in batches
                for batch in parquet_file_obj.iter_batches(batch_size=batch_size):
                    batches_processed += 1
                    batch_df = batch.to_pandas()
                    records_processed += len(batch_df)
                    
                    print(f"  Processing: {records_processed:,} / {total_rows:,} records ({records_processed*100//total_rows}%)", end='\r')
                    
                    # Convert batch to list of dicts
                    records = batch_df.to_dict('records')
                    
                    # Enrich and write each record
                    for record in records:
                        enriched_record = enrich_record(record, zone_dict)
                        
                        if not first_record:
                            f.write(',\n')
                        else:
                            f.write('\n')
                            first_record = False
                        
                        json.dump(enriched_record, f, indent=2, default=str)
                    
                    # Free memory
                    del batch_df, records
                
                f.write('\n]')  # End JSON array
            
            print(f"\n  ✓ Saved to: {json_path}")
            
        except Exception as e:
            print(f"\n  ✗ Error processing {parquet_file.name}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    # Configuration
    INPUT_FOLDER = "datasets_parquet"
    OUTPUT_FOLDER = "datasets_json"
    SHAPEFILE_PATH = "taxi_zones/taxi_zones.shp"
    BATCH_SIZE = 50000  # Process 50k records at a time (reduced for better memory management)
    
    # Check if shapefile exists
    if not os.path.exists(SHAPEFILE_PATH):
        print(f"Error: Shapefile not found at {SHAPEFILE_PATH}")
        exit(1)
    
    # Check if input folder exists
    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Input folder not found at {INPUT_FOLDER}")
        exit(1)
    
    # Run conversion
    convert_parquet_to_json_chunked(INPUT_FOLDER, OUTPUT_FOLDER, SHAPEFILE_PATH, BATCH_SIZE)
    print("\n✓ Conversion complete!")