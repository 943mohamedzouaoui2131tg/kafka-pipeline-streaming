from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27019"   # mongos exposed port
MONGO_DATABASE = "BDABD"
MONGO_COLLECTION = "taxi_events"

print(f"🔌 Connecting to MongoDB at {MONGO_URI}")

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000
)

client.admin.command("ping")
print("✅ Connected to mongos router")

db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]
