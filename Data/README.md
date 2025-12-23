## Installation
# Step 1 : Install Required Packages

Open your terminal/command prompt and run:

```bash
pip install pandas pyarrow geopandas
```

# Step 2 : Run script

Open your terminal/command prompt and run:

```bash
py parquet_to_json.py
```
# Folder Structure :

Data/
├── parquet_to_json.py                      # The conversion script
├── datasets_parquet/              # Input folder with .parquet files
│   ├── trips_2024_01.parquet
│   ├── trips_2024_02.parquet
│   └── ...
├── taxi_zones/                    # Taxi zone shapefile folder (exists)
│   ├── taxi_zones.shp
│   ├── taxi_zones.shx
│   ├── taxi_zones.dbf
│   └── taxi_zones.prj
└── datasets_json/                 # Output folder (created automatically)