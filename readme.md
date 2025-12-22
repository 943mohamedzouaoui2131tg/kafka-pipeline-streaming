# Guide de démarrage du projet Kafka-Producer/Consumer

Ce projet contient un producteur Kafka, deux consommateurs (MongoDB et Cassandra),

## Prérequis

-   Docker et Docker Compose installés

## Structure du projet

-   `Producer/` : Producteur Kafka (Python)
-   `mongo_consumer/` : Consommateur MongoDB (Python)
-   `cassandra_consumer/` : Consommateur Cassandra (Python)
-   `Brokers/` : Infrastructure Kafka/Zookeeper (Docker Compose)

## Étapes de démarrage

### 1. Démarrer l’infrastructure Kafka

Ouvrez un terminal, placez-vous dans le dossier `Brokers/` puis lancez :

```bash
cd Brokers
# Démarrer Kafka, Zookeeper, etc.
docker-compose up -d
```

Vérifiez que les services sont bien démarrés :

```bash
docker-compose ps
```

### 3. Démarrer le producteur

Dans un autre terminal :

```bash
docker exec -it producer bash

python3 ./src/producer.py
```

### 4. Démarrer les consommateurs

#### MongoDB Consumer

```bash

docker exec -it mongo_consumer bash

python3 ./src/producer.py

```

#### Cassandra Consumer

```bash
docker exec -it cassandra1 cqlsh


python3 ./src/producer.py
```

### 5. Arrêt de l’infrastructure

Pour tout arrêter :

```bash
cd ../Brokers
docker-compose down
```
