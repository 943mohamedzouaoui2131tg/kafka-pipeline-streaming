kafka-topics.sh --create --topic=taxi_raw --partitions=6 --replication-factor=2 --zookeeper=zookeeper:2181

# kafka-topics.sh --describe --topic=taxi_raw --zookeeper=zookeeper:2181