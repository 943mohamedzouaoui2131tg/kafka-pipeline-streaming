# Guide de démarrage du Projet Pipeline Streaming Taxi avec Kafka, MongoDB et Cassandra

`L’objectif de projet ce projet` concevoir, implémenter et tester un pipeline Big Data temps réel pour le traitement de flux de données issues du service de taxis de New York , L’idée est:

-   utiliser Kafka pour la gestion du streaming
     - comparerez deux bases NoSQL distribuées performantes, MongoDB et Cassandra

## Prérequis

-   Docker et Docker Compose installés

## Structure du projet

```text
.
├── .gitignore
├── README.md
│
├── Brokers
│   └── docker-compose.yml
│       # Le fichier Docker Compose définit les services
│       # pour la création de deux brokers Kafka, des consommateurs
│       # MongoDB et Cassandra, ainsi que le producteur.
│
├── cassandra-compose
│   ├── cassandra-init-Rf1.sh
│   ├── cassandra-init-Rf2.sh
│   ├── cassandra-init-Rf3.sh
│   ├── data_choisi.txt
│   └── docker-compose.yml
│
├── cassandra_consumer
│   ├── .dockerignore
│   ├── .env
│   ├── Dockerfile
│   │   # Le fichier Docker contient les instructions
│   │   # pour construire l'image
│   ├── requirements.txt
│   │   # Liste les dépendances nécessaires pour démarrer le conteneur
│   └── src
│       └── consumer.py
│           # Script Python pour lancer le consumer Cassandra
│
├── Data
│   # Les données
│   ├── parquet_to_json.py
│   ├── README.md
│   ├── datasets_json
│   │   ├── dd.yml
│   │   ├── test.json
│   │   ├── yellow_tripdata_2024-01.json
│   │   └── yellow_tripdata_2024-02.json
│   ├── datasets_parquet
│   │   ├── yellow_tripdata_2024-01.parquet
│   │   └── yellow_tripdata_2024-02.parquet
│   └── taxi_zones
│       ├── taxi_zones.dbf
│       ├── taxi_zones.mxd
│       ├── taxi_zones.prj
│       ├── taxi_zones.sbn
│       ├── taxi_zones.sbx
│       ├── taxi_zones.shp
│       └── taxi_zones.shx
│
├── mongo-compose
│   ├── docker-compose.yml
│   │   # Fichier Docker Compose pour configurer les serveurs MongoDB
│   └── mongo-up.sh
│   └── mongo-down.sh # cleanup des shards
├── mongo-compose-6-shards
│   ├── docker-compose.yml
│   │   # Fichier Docker Compose pour configurer les serveurs MongoDB
│   └── mongo-up-6-shards.sh # création de base mongo en structure 6 shards
│   └── mongo-cleanup-6shards.sh # cleanup de base mongo en structure 6 shards
│
├── mongo_consumer
│   ├── .dockerignore
│   ├── .env
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src
│       └── consumer.py
│           # Script Python pour lancer le consumer MongoDB
├── Web_App
│   ├── .. # structure de l'application (pour détails coir Web_app\readme.md)
│   ├── readme.md
│   ├── Dockerfile
│   └── requirements.txt
│   
│
└── Producer
    # Fichiers du producteur
    ├── .dockerignore
    ├── .env
    ├── Dockerfile
    │   # Image pour construire le producteur
    ├── requirements.txt
    │   # Bibliothèques nécessaires pour démarrer le conteneur
    └── src
        └── producer.py
            # Script Python pour lancer le producteur et produire des données

```

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

On exécute toujours le script cp.sh pour copier ces données depuis notre ordinateur vers le conteneur du producteur afin de les diffuser (streaming)

```bash

cd Data

bash cp.sh # ou .\cp.sh

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

### 7. Démarrer le producteur

Dans un autre terminal :

```bash

docker exec -it producer bash

python3 ./src/producer.py

```

### 8. Démarrer les consommateurs

#### MongoDB Consumer

```bash



docker exec -it mongo_consumer bash

python3 ./src/consumer.py



```

#### Cassandra Consumer

```bash

cd cassandra-compose

docker-compose up -d

bash cassandra-init-Rf1.sh
#Powershell:
& "C:\Program Files\Git\bin\bash.exe" cassandra-init-Rf1.sh

bash cassandra-init-Rf2.sh
#Powershell:
& "C:\Program Files\Git\bin\bash.exe" cassandra-init-Rf2.sh

bash cassandra-init-Rf3.sh
#Powershell:
& "C:\Program Files\Git\bin\bash.exe" cassandra-init-Rf3.sh

```

#### Lancement du Consumer Cassandra

```bash

docker exec -it cassandra_consumer bash

python3 ./src/consumer.py

```
### 9. Connecter les conteneurs au réseau

Une fois les conteneurs démarrés, connectez-les au réseau `abd` :

```bash

docker network connect abd cassandra1

docker network connect abd cassandra_consumer 
```

### 10. Arrêt de l’infrastructure

Pour tout arrêter :

```bash

cd ../Brokers

docker-compose down

```

```bash

cd ../mongo_compose

docker-compose down

```
