from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27019")
MONGO_DATABASE   = "BDABD"
MONGO_COLLECTION = "taxi_events"

print(f"🔌 Connecting to MongoDB at {MONGO_URI}")

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,   # 5s pour trouver un serveur
    socketTimeoutMS=10000,           # 10s timeout par opération
    connectTimeoutMS=5000,           # 5s timeout de connexion TCP
    retryWrites=True,                # pymongo réessaie automatiquement (comportement normal)
    retryReads=True,
    # heartbeatFrequencyMS par défaut = 10s, c'est correct
)

# On vérifie la connexion en mode "best effort" — si ça échoue on continue
# quand même, les routes vont gérer leurs propres erreurs.
try:
    client.admin.command("ping")
    print("✅ Connected to mongos router")
except Exception as e:
    print(f"⚠️  Ping échoué au démarrage ({e}) — l'app continue quand même")

db         = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]