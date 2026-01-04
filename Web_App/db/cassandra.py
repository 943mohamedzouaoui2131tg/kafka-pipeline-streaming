from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import os

# Get Cassandra contact points from environment or use default
# cassandra1 exposes CQL port 9042 as 3000 on host
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "3000"))  # cassandra1's mapped CQL port (3000 -> 9042)
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "projet_bd_rf3")  # Use RF1 keyspace

print(f"🔌 Connecting to Cassandra cluster at {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")
print(f"📚 Using keyspace: {CASSANDRA_KEYSPACE}")

cluster = Cluster(
    contact_points=CASSANDRA_HOSTS,
    port=CASSANDRA_PORT,
    load_balancing_policy=RoundRobinPolicy(),
    protocol_version=4  # Changed from 3 to 4 for better compatibility
)

try:
    session = cluster.connect()
    
    # Set keyspace with proper case (Cassandra is case-sensitive with quotes)
    # Try the exact keyspace name from your scripts
    keyspace_name = CASSANDRA_KEYSPACE
    
    # First, try to verify the keyspace exists
    keyspaces_query = "SELECT keyspace_name FROM system_schema.keyspaces"
    existing_keyspaces = session.execute(keyspaces_query)
    keyspace_list = [row.keyspace_name for row in existing_keyspaces]
    
    print(f"📋 Available keyspaces: {keyspace_list}")
    
    # Check if our keyspace exists (case-insensitive check)
    keyspace_exists = any(ks.lower() == keyspace_name.lower() for ks in keyspace_list)
    
    if keyspace_exists:
        # Find the exact case
        exact_keyspace = next(ks for ks in keyspace_list if ks.lower() == keyspace_name.lower())
        session.set_keyspace(exact_keyspace)
        print(f"✅ Connected to keyspace: {exact_keyspace}")
    else:
        print(f"⚠️ Keyspace '{keyspace_name}' not found. Available keyspaces: {keyspace_list}")
        print(f"⚠️ Using session without keyspace. You may need to specify keyspace in queries.")
        
except Exception as e:
    print(f"❌ Error connecting to Cassandra: {e}")
    raise