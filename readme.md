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

### 1. Création du réseau Docker

Avant de démarrer les services, créez un réseau Docker dédié :

```bash
docker network create abd
```

### 2. Démarrer l’infrastructure Kafka

Ouvrez un terminal, placez-vous dans le dossier `Brokers/` puis lancez :

```bash
cd Brokers
docker-compose up -d
```

### 3. Démarrer le service MongoDB

Dans un autre terminal, placez-vous dans le dossier `mongo_compose/` puis lancez :

```bash
cd ../mongo_compose
docker-compose up -d
```

### 4. Connecter les conteneurs au réseau

Une fois les conteneurs démarrés, connectez-les au réseau `abd` :

```bash
docker network connect abd mongos
docker network connect abd mongo_consumer
```

### 5. Initialiser le sharding MongoDB

Exécutez le script bash `mongo-up.sh` pour créer les shards de MongoDB :

```bash
bash mongo-up.sh
```

### 4. Démarrer le producteur

Dans un autre terminal :

```bash
docker exec -it producer bash

python3 ./src/producer.py
```

### 5. Démarrer les consommateurs

#### MongoDB Consumer

```bash

docker exec -it mongo_consumer bash

python3 ./src/consumer.py

```

#### Cassandra Consumer

```bash
docker exec -it cassandra_consumer bash


python3 ./src/consumer.py
```

### 6. Arrêt de l’infrastructure

Pour tout arrêter :

```bash
cd ../Brokers
docker-compose down
```

```bash
cd ../mongo_compose
docker-compose down
```
