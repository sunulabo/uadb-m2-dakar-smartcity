# scripts/train_and_catalog.py — Entraînement + Versionnage modèle Dakar
# Prédit la vitesse attendue (speed_kmh) selon heure, ligne, type de véhicule

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import hour, dayofweek, col
import datetime
import subprocess
import json

# ── SparkSession ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('DakarSmartCity_Train') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── Chargement données historiques (table Hive Gold) ──────────
df = spark.table('dakar_smart_city.mobilite_gold').cache()
print(f'Données chargées : {df.count()} enregistrements')

# ── Feature Engineering ────────────────────────────────────────
df = (
    df
    .withColumn('heure',        hour(col('event_ts')))
    .withColumn('jour_semaine', dayofweek(col('event_ts')))
    .filter(col('speed_kmh').isNotNull())
)

NUM_COLS = ['latitude', 'longitude', 'passagers', 'heure', 'jour_semaine']
CAT_COLS = ['vehicle_type', 'ligne_id']

# Encodage des variables catégorielles
indexers = [
    StringIndexer(inputCol=c, outputCol=c + '_idx', handleInvalid='skip')
    for c in CAT_COLS
]

# Assemblage des features
assembler = VectorAssembler(
    inputCols=NUM_COLS + [c + '_idx' for c in CAT_COLS],
    outputCol='unscaled_features',
    handleInvalid='skip'
)

# Normalisation
scaler = StandardScaler(
    inputCol='unscaled_features',
    outputCol='features',
    withStd=True,
    withMean=True
)

# Modèle RandomForest
rf = RandomForestRegressor(
    featuresCol='features',
    labelCol='speed_kmh',
    numTrees=100,
    maxDepth=8,
    seed=42
)

pipeline = Pipeline(stages=indexers + [assembler, scaler, rf])

# ── Entraînement ───────────────────────────────────────────────
train, test = df.randomSplit([0.8, 0.2], seed=42)
train.cache()
test.cache()

print('Entraînement RandomForest en cours...')
model = pipeline.fit(train)
print('Entraînement terminé ✓')

# ── Évaluation ─────────────────────────────────────────────────
preds = model.transform(test)

ev_rmse = RegressionEvaluator(labelCol='speed_kmh', predictionCol='prediction', metricName='rmse')
ev_r2   = RegressionEvaluator(labelCol='speed_kmh', predictionCol='prediction', metricName='r2')

rmse = ev_rmse.evaluate(preds)
r2   = ev_r2.evaluate(preds)

print(f'RandomForest | RMSE : {rmse:.2f} km/h | R² : {r2:.4f}')

# Feature importance
feat_names  = NUM_COLS + CAT_COLS
importances = model.stages[-1].featureImportances.toArray()
print('\nImportance des features :')
for nm, imp in sorted(zip(feat_names[:len(importances)], importances), key=lambda x: -x[1]):
    print(f'  {nm:30s} : {imp:.4f}')

# ── Versionnage du modèle (Model Registry HDFS) ────────────────
version     = datetime.datetime.now().strftime('%Y%m%d_%H%M')
model_path  = f'/opt/uadb/models/traffic_v_{version}'
latest_path = '/opt/uadb/models/latest'

model.write().overwrite().save(model_path)

# Mettre à jour le lien 'latest' (copie HDFS)
subprocess.run(['hdfs', 'dfs', '-rm', '-r', '-f', latest_path], check=False)
subprocess.run(['hdfs', 'dfs', '-cp', model_path, latest_path], check=True)

print(f'Modèle sauvegardé : {model_path} (latest mis à jour)')

# Sauvegarder les métriques pour monitoring drift
metrics = {'version': version, 'rmse': rmse, 'r2': r2}
metrics_path = f'/opt/uadb/models/metrics_{version}.json'
with open(metrics_path, 'w') as f:
    json.dump(metrics, f, indent=2)

print(f'Métriques sauvegardées : {metrics_path}')

train.unpersist()
test.unpersist()
df.unpersist()
spark.stop()
