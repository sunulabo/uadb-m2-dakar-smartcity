# streaming_core.py — Pipeline Spark Streaming Dakar Smart City
# Privacy Layer (SHA-256) + Inférence vitesse + Détection congestion

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat, lit, from_json, to_json, struct,
    window, avg, count, when, coalesce, current_timestamp, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)
from pyspark.ml import PipelineModel

# ── Configuration ──────────────────────────────────────────────
SECRET_SALT = os.environ.get('DAKAR_SECRET_SALT', 'fallback_salt_uadb_2025')
BROKERS     = 'kafka:9092'
MODEL_PATH  = '/opt/uadb/models/latest'

# ── SparkSession ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('DakarSmartCity_Streaming') \
    .config('spark.sql.shuffle.partitions', '4') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── Schéma GPS véhicules ───────────────────────────────────────
gps_schema = StructType([
    StructField('vehicle_id',   StringType(),  True),
    StructField('vehicle_type', StringType(),  True),
    StructField('latitude',     FloatType(),   True),
    StructField('longitude',    FloatType(),   True),
    StructField('speed_kmh',    FloatType(),   True),
    StructField('heading_deg',  FloatType(),   True),
    StructField('ligne_id',     StringType(),  True),
    StructField('passagers',    IntegerType(), True),
    StructField('timestamp',    StringType(),  True),
])

# ── Lecture Kafka ──────────────────────────────────────────────
raw_df = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('subscribe', 'dakar_mobility_clean')
    .option('startingOffsets', 'latest')
    .load()
    .select(from_json(col('value').cast('string'), gps_schema).alias('d'))
    .select('d.*')
    .withColumn('event_ts', current_timestamp())
)

# ── Privacy Layer ──────────────────────────────────────────────
# Hachage SHA-256 de la plaque — irréversible sans le sel
secure_df = (
    raw_df
    .withColumn('vehicle_secure',
                sha2(concat(col('vehicle_id'), lit(SECRET_SALT)), 256))
    .drop('vehicle_id')   # Suppression OBLIGATOIRE de la plaque brute
    # Protection contre les NULL GPS
    .withColumn('latitude',  coalesce(col('latitude'),  lit(14.693)))
    .withColumn('longitude', coalesce(col('longitude'), lit(-17.447)))
    .withColumn('speed_kmh', coalesce(col('speed_kmh'), lit(0.0)))
    .withColumn('passagers', coalesce(col('passagers'), lit(0)))
)

# ── Inférence ML : prédiction vitesse attendue ────────────────
try:
    model = PipelineModel.load(MODEL_PATH)
    scored_df = model.transform(secure_df)
    print(f'Modèle chargé depuis {MODEL_PATH}')
except Exception as e:
    print(f'⚠ Modèle non disponible ({e}) — mode règles expertes')
    # Fallback : règle experte basée sur l'heure de la journée
    scored_df = secure_df.withColumn(
        'speed_predicted',
        when(
            (hour(current_timestamp()).between(7, 9)) |
            (hour(current_timestamp()).between(17, 20)),
            lit(8.0)
        ).otherwise(lit(25.0))
    )

# ── Détection congestion (fenêtre 15min, watermark 5min) ──────
congestion_df = (
    scored_df
    .withWatermark('event_ts', '5 minutes')
    .groupBy(window('event_ts', '15 minutes', '5 minutes'), 'ligne_id')
    .agg(
        avg('speed_kmh').alias('vitesse_moy'),
        avg(coalesce('speed_predicted', lit(25.0))).alias('vitesse_predite'),
        count('vehicle_secure').alias('nb_vehicules'),
        avg('passagers').alias('passagers_moy'),
    )
    .withColumn('ecart_vitesse',
                coalesce(col('vitesse_predite'), lit(25.0)) -
                coalesce(col('vitesse_moy'), lit(25.0)))
    .withColumn('niveau_congestion',
                when(col('vitesse_moy') < 5,  lit('CRITIQUE'))
                .when(col('vitesse_moy') < 15, lit('MODERE'))
                .otherwise(lit('FLUIDE')))
)

# ── Query 1 : Alertes trafic → Kafka ──────────────────────────
q1 = (
    congestion_df
    .select(to_json(struct('*')).alias('value'))
    .writeStream.format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'dakar_trafic_alerts')
    .option('checkpointLocation', '/tmp/dakar_congestion_ckpt')
    .outputMode('update')
    .start()
)

# ── Query 2 : Données enrichies → Hive Gold Layer ─────────────
q2 = (
    scored_df
    .select('vehicle_secure', 'vehicle_type', 'ligne_id',
            'latitude', 'longitude', 'speed_kmh',
            'passagers', 'event_ts')
    .writeStream.format('parquet')
    .option('path', 'hdfs:///dakar_smart_city/gold/')
    .option('checkpointLocation', '/tmp/dakar_gold_ckpt')
    .outputMode('append')
    .start()
)

q1.awaitTermination()
