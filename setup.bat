@echo off
REM setup.bat — Démarrage Dakar Smart City sur Windows
REM Exécuter dans le dossier du projet avec Docker Desktop lancé

echo ============================================
echo   Dakar Smart City — Démarrage Infrastructure
echo ============================================

echo.
echo [1/5] Démarrage Zookeeper...
docker compose up -d zookeeper
timeout /t 10 /nobreak

echo.
echo [2/5] Démarrage Kafka, NiFi, HBase, Hive...
docker compose up -d kafka nifi hbase hive-metastore
timeout /t 30 /nobreak

echo.
echo [3/5] Démarrage Spark et Airflow...
docker compose up -d spark-master spark-worker airflow
timeout /t 15 /nobreak

echo.
echo [4/5] Vérification des containers...
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo.
echo [5/5] Interfaces disponibles :
echo   NiFi    : http://localhost:8081  (OBLIGATOIRE soutenance)
echo   Spark   : http://localhost:8080
echo   HBase   : http://localhost:16010
echo   Airflow : http://localhost:8082  (admin/admin)
echo.
echo Prochaine étape : initialiser HBase et lancer le simulateur GPS
echo   python hbase_setup.py
echo   python kafka_producer_dakar.py
echo.
pause
