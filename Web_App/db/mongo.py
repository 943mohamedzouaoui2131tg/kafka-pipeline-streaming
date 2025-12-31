from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27019"  # use the mapped port
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client.BDABD
collection = db.taxi_events
