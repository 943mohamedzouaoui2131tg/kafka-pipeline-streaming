from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

cluster = Cluster(
    contact_points=["127.0.0.1"],
    port=3000,
    load_balancing_policy=RoundRobinPolicy(),
    protocol_version=3
)

session = cluster.connect()        # NO keyspace here
session.set_keyspace("projet_bd_rf3")
