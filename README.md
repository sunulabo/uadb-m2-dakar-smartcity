# 🚌 Dakar Smart City — Système Temps-Réel de Mobilité Urbaine

**UADB | Master 2 Big Data & IA | 2025-2026**  
Enseignant : Mr Ahmed Ben Sidy Bouya SEYE | Senior Big Data & AI Engineer | Groupe Sonatel

---

## 📁 Structure du Projet

```
dakar-smart-city/
├── docker-compose.yml          # Infrastructure complète
├── requirements.txt            # Dépendances Python
├── hbase_setup.py              # Initialisation tables HBase
├── schema.py                   # Contrat de données Pandera
├── kafka_producer_dakar.py     # Simulateur GPS véhicules
├── streaming_core.py           # Pipeline Spark Streaming
├── hive_setup.sql              # Tables et vues Hive
├── dashboard_mobilite.py       # Dashboard 4 panneaux
├── validation_nifi.sh          # Validation NiFi
├── setup.bat                   # Script démarrage Windows
├── dags/
│   └── dakar_smart_city_dag.py # DAG Airflow MLOps
├── scripts/
│   └── train_and_catalog.py    # Entraînement RandomForest
├── data/                       # Données temporaires
├── models/                     # Modèles versionnés
└── nifi_templates/             # Templates NiFi XML
```

---

## ⚙️ Prérequis (Windows)

- **Docker Desktop** installé et lancé
- **WSL2** activé (recommandé)
- **RAM** : 16 Go minimum
- **CPU** : 4 cœurs minimum

---

## 🚀 Démarrage Rapide (Windows)

### Étape 1 — Lancer l'infrastructure Docker

```bash
# Dans un terminal PowerShell ou CMD, dans le dossier du projet :
docker compose up -d zookeeper
# Attendre 10 secondes...
docker compose up -d kafka nifi hbase hive-metastore
# Attendre 30 secondes...
docker compose up -d spark-master spark-worker airflow
```

Vérifier que tout tourne :
```bash
docker ps
```

### Étape 2 — Initialiser HBase

```bash
docker exec spark-master python /opt/uadb/scripts/hbase_setup.py
# Ou localement : pip install happybase && python hbase_setup.py
```

### Étape 3 — Créer les tables Hive

```bash
docker exec hive-metastore hive -f /opt/hive/scripts/hive_setup.sql
```

### Étape 4 — Démarrer le simulateur GPS

```bash
# Dans un nouveau terminal :
docker exec spark-master python /opt/uadb/scripts/kafka_producer_dakar.py
```

### Étape 5 — Lancer le pipeline Spark Streaming

```bash
spark-submit --master spark://spark-master:7077 streaming_core.py
```

### Étape 6 — Entraînement initial du modèle

```bash
# Après accumulation de données (attendre ~5 min) :
spark-submit --master spark://spark-master:7077 scripts/train_and_catalog.py
```

---

## 🌐 Interfaces Web

| Service  | URL                       | Note                                  |
|----------|---------------------------|---------------------------------------|
| **NiFi** | http://localhost:8081     | ⚠️ Obligatoire en soutenance           |
| Spark    | http://localhost:8080     | Monitoring jobs streaming             |
| HBase    | http://localhost:16010    | Vue tables et données                 |
| Airflow  | http://localhost:8082     | admin / admin — DAG monitoring        |

---

## 🔧 Configuration NiFi (Process Group : Dakar_Mobility_Ingestion)

1. Ouvrir http://localhost:8081
2. Créer un **Process Group** nommé `Dakar_Mobility_Ingestion`
3. Ajouter les processeurs suivants :

| Processeur        | Paramètre             | Valeur                                          |
|-------------------|-----------------------|-------------------------------------------------|
| GetFile/ListenHTTP| Input Directory/Port  | `/opt/nifi/data/` \| Port `8888`                |
| UpdateAttribute   | data.origin           | `dakar_sensor`                                  |
| UpdateAttribute   | data.timestamp        | `${now():format('yyyy-MM-dd\'T\'HH:mm:ssZ')}`   |
| PublishKafka_2_6  | Kafka Brokers         | `kafka:9092`                                    |
| PublishKafka_2_6  | Topic Name            | `dakar_mobility_clean`                          |
| PublishKafka_2_6  | Message Key           | `${vehicle_id}`                                 |

4. Démarrer tous les processeurs
5. **Exporter le template** : clic droit → Download flow definition → `template_nifi_dakar_equipeX.xml`

---

## 📊 Barème de Notation (20 pts)

| Critère                    | Pts | Objectif Excellence                                              |
|----------------------------|-----|------------------------------------------------------------------|
| Infrastructure & Setup NiFi| 3   | Process Group actif, messages JSON visibles sur port 8081        |
| Validation & Privacy        | 4   | DakarMobilitySchema + SHA-256 + drop(vehicle_id) + COALESCE      |
| Streaming & Kafka           | 3   | 2 queries indépendantes + watermarks + fallback modèle           |
| MLOps & IA                  | 6   | RF + versionnage + BranchPython RMSE>15 + réentraînement auto    |
| HBase/Hive & Analytics      | 4   | vue_etat_trafic + vue_model_drift + alertes HBase live + dashboard|

**Bonus** : README + captures NiFi (+0.5), docstrings (+0.5), MLflow (+0.5), démo live HBase (+0.5)

---

## 🏗️ Architecture End-to-End

```
[ Simulateur GPS ] → (Pandera) → [ NiFi ] → [ Kafka ]
                                               ↓
                                        [ Spark Streaming ]
                                         ├── SHA-256 Privacy
                                         ├── Inférence RF
                                         └── Détection Congestion
                                               ↓              ↓
                                          [ HBase ]        [ Hive ]
                                        (temps réel)    (Gold Layer)
                                               ↓
                                          [ Airflow ]
                                        (DAG Monitoring)
                                         ├── Check RMSE drift
                                         ├── BranchPython RMSE>15
                                         └── Réentraînement auto
```

---
